package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.UserRegisterBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 用户域用户注册聚合汇总表
 *
 * @author pangzl
 * @create 2022-07-13 11:47
 */
public class DwsUserUserRegisterWindow {

    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 读取 Kafka dwd_user_register 主题数据，封装为流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";
        FlinkKafkaConsumer<String> kafkaConsumer =
                MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS =
                mappedStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject jsonObj,
                                                                         long recordTimestamp) {
                                                return jsonObj.getLong("ts") * 1000L;
                                            }
                                        }
                                )
                );

        // TODO 6. 开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 7. 聚合
        SingleOutputStreamOperator<UserRegisterBean> aggregateDS =
                windowDS.aggregate(
                        new AggregateFunction<JSONObject, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(JSONObject jsonObj, Long accumulator) {
                                accumulator += 1;
                                return accumulator;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new AllWindowFunction<Long, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<Long> values,
                                              Collector<UserRegisterBean> out) throws Exception {
                                for (Long value : values) {
                                    String stt = DateFormatUtil.toYmdHms(window.getStart());
                                    String edt = DateFormatUtil.toYmdHms(window.getEnd());
                                    UserRegisterBean userRegisterBean = new UserRegisterBean(
                                            stt,
                                            edt,
                                            value,
                                            System.currentTimeMillis()
                                    );
                                    out.collect(userRegisterBean);
                                }
                            }
                        }
                );

        // TODO 8. 写入到 OLAP 数据库ClickHouse
        SinkFunction<UserRegisterBean> sinkFunction =
                ClickHouseUtil.<UserRegisterBean>getJdbcSink(
                        "insert into dws_user_user_register_window values(?,?,?,?)"
                );
        aggregateDS.addSink(sinkFunction);

        env.execute();
    }
}
