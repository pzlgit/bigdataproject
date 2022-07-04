package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 流量域 用户跳出 事务事实表
 *
 * @author pangzl
 * @create 2022-07-04 20:25
 */
public class DwdTrafficUserJumpDetail {

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

        // TODO 3.从Kafka中读取数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> kafkaStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // 测试数据
//        DataStream<String> kafkaStrDS = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":15000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":30000} "
//                );

        // TODO 4.转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedDS = kafkaStrDS.map(JSON::parseObject);

        mappedDS.print("---");

        // TODO 5.设置水位线和指定事件时间字段，用于用户跳出统计
        SingleOutputStreamOperator<JSONObject> watermarkDS = mappedDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 6.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream =
                watermarkDS.keyBy(r -> r.getJSONObject("common").getString("mid"));

        // TODO 7.使用Flink CEP 复杂事件处理提取复杂事件
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        ).within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L));

        // 8. 把pattern应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(keyedStream, pattern);

        // 9. 提取匹配上的事件以及超时时间
        // 定义超时事件数据侧输出流标签
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag") {
        };
        SingleOutputStreamOperator<JSONObject> resultDS = patternDS.flatSelect(
                timeoutTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<JSONObject> collector) throws Exception {
                        // 处理超时事件数据
                        JSONObject first = map.get("first").get(0);
                        // 虽然调用的是collect方法，但是依旧是将数据放入到参数1定义的测输出流timeoutTag中
                        collector.collect(first);
                    }
                },
                new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<JSONObject> collector) throws Exception {
                        // 处理正常匹配上的数据
                        JSONObject first = map.get("first").get(0);
                        // 将数据放入到主流
                        collector.collect(first);
                    }
                }
        );

        // 将超时数据从测输出流中读取到
        DataStream<JSONObject> timeOutDS = resultDS.getSideOutput(timeoutTag);
        // 11.合并两个流，并将数据写出到Kafka的dwd_traffic_user_jump_detail
        DataStream<JSONObject> union = resultDS.union(timeOutDS);

        union.print(">>>");

        union.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_user_jump_detail"));

        env.execute();
    }
}
