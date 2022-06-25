package com.atguigu.chapter09;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 对用户点击流每5个数据统计一次平均时间戳
 *
 * @author pangzl
 * @create 2022-06-25 20:44
 */
public class AverageTimestampExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream =
                env.addSource(new ClickSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Event>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<Event>() {
                                                    @Override
                                                    public long extractTimestamp(Event element,
                                                                                 long recordTimestamp) {
                                                        return element.timestamp;
                                                    }
                                                })
                        );
        // 统计每个用户的点击频次，到达5此就输出统计结果
        stream.keyBy(r -> r.user)
                .flatMap(new RichFlatMapFunction<Event, String>() {

                    // 定义状态，用来保存计算平均时间戳的数据
                    private AggregatingState<Event, Long> avgTsAggState;

                    // 定义值状态，用来保存当前用户访问频次
                    private ValueState<Long> countState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        avgTsAggState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                                        "avg-ts",
                                        new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                            @Override
                                            public Tuple2<Long, Long> createAccumulator() {
                                                return Tuple2.of(0L, 0L);
                                            }

                                            @Override
                                            public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value.timestamp,
                                                        accumulator.f1 + 1L);
                                            }

                                            @Override
                                            public Long getResult(Tuple2<Long, Long> accumulator) {
                                                return (long) accumulator.f0 / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                                return null;
                                            }
                                        },
                                        Types.TUPLE(Types.LONG, Types.LONG)
                                )
                        );

                        countState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("count", Long.class));
                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        Long count = countState.value();
                        if (count == null) {
                            count = 1L;
                        } else {
                            count++;
                        }

                        countState.update(count);
                        avgTsAggState.add(value);

                        // 达到5次就输出结果，并清空状态
                        if (count == 5) {
                            out.collect(value.user + " 平均时间戳：" +
                                    new Timestamp(avgTsAggState.get()));
                            countState.clear();
                        }
                    }
                }).print();

        env.execute();
    }
}
