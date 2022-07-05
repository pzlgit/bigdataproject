package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * 流量域独立访客事务事实表
 * <p>
 * 独立访客（UV）: 同一mid的当天的访问算作一个独立访客
 *
 * @author pangzl
 * @create 2022-07-04 19:09
 */
public class DwdTrafficUniqueVisitorDetail {

    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 检查点相关设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig
                        .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:9820/gmall/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 kafka dwd_traffic_page_log 主题读取日志数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageStrDS = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构 jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = pageStrDS.map(JSON::parseObject);

        // 测试能否消费到数据
        //jsonObjDS.print(">>>>");

        // TODO 5. 按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream =
                jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 6. 通过Flink状态编程过滤独立访客记录
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {

                    // 定义值状态，用来存放当前mid的上次访问日期
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 构建值状态描述器
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>(
                                "lastVisitDateState", String.class
                        );
                        // 设置状态存活时间TTL
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1))
                                        // 设置过期数据是否可见:从不返回过期数据
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        // 设置在创建和更新状态时，存活时间重新开始记时
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        // 获取值状态
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        // 过滤last_page_id 不为null的数据
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (!StringUtils.isEmpty(lastPageId)) {
                            return false;
                        }
                        Long ts = jsonObj.getLong("ts");
                        String curTsDate = DateFormatUtil.toDate(ts);
                        String lastVisitDate = lastVisitDateState.value();
                        // 利用状态编程判断当前mid是否是独立访客
                        if (lastVisitDate == null || !lastVisitDate.equals(curTsDate)) {
                            lastVisitDateState.update(curTsDate);
                            return true;
                        }
                        return false;
                    }
                }
        );

        filterDS.print(">>>");

        // TODO 7. 将独立访客数据写入到Kafka的主题dwd_traffic_unique_visitor_detail
        filterDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_unique_visitor_detail"));

        env.execute();
    }
}
