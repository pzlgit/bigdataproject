package com.atguigu.exec;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import com.google.common.base.Charsets;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 使用布隆过滤器求每个用户访问url的个数需求实现
 *
 * @author pangzl
 * @create 2022-06-24 11:16
 */
public class Example2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                ).keyBy(e -> e.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg(), new WindowResult())
                .print();
        env.execute();
    }

    // 自定义聚合函数实现求每个用户的url个数
    public static class CountAgg implements AggregateFunction<Event, Tuple2<BloomFilter<String>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<String>, Long> createAccumulator() {
            return Tuple2.of(
                    BloomFilter.create(
                            Funnels.stringFunnel(Charsets.UTF_8),
                            10000,
                            0.01
                    ),
                    0L
            );
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> add(Event value, Tuple2<BloomFilter<String>, Long> accumulator) {
            if (!accumulator.f0.mightContain(value.url)) {
                accumulator.f0.put(value.url);
                accumulator.f1 += 1L;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<String>, Long> merge(Tuple2<BloomFilter<String>, Long> a, Tuple2<BloomFilter<String>, Long> b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context ctx, Iterable<Long> elements, Collector<String> out) throws Exception {
            out.collect("窗口：" + new Timestamp(ctx.window().getStart()) + "~" +
                    "" + new Timestamp(ctx.window().getEnd()) + "的uv是：" +
                    "" + elements.iterator().next());
        }
    }
}
