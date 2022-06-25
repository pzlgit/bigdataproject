package com.atguigu.chapter08;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 多流联合 union 水位线测试
 *
 * @author pangzl
 * @create 2022-06-24 20:46
 */
public class UnionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取到两个socket文本流
        SingleOutputStreamOperator<Event> stream1 = env.socketTextStream("localhost", 7777)
                .map(line -> {
                    String[] words = line.split(",");
                    return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );

        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("localhost", 8888)
                .map(line -> {
                    String[] words = line.split(",");
                    return new Event(words[0].trim(), words[1].trim(), Long.valueOf(words[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );

        // 两个流联合
        SingleOutputStreamOperator<String> process = stream1.union(stream2)
                .process(
                        new ProcessFunction<Event, String>() {
                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                                out.collect("水位线：" + ctx.timerService().currentWatermark());
                            }
                        }
                );

        process.print();
        env.execute();
    }
}
