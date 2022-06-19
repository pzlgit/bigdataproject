package com.atguigu.transform;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * FlatMap 扁平映射
 *
 * @author pangzl
 * @create 2022-06-18 20:34
 */
public class FlatMapExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        // 1.匿名函数类实现FlatMap
        stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                collector.collect(event.user);
                collector.collect(event.url);
            }
        }).print("匿名函数");
        // 2.自定义类实现FlatMap
        stream.flatMap(new MyFlatMapFunction()).print("自定义类");
        // 3.Lambda实现FlatMap
//        SingleOutputStreamOperator<Object> returns = stream.flatMap((event, collector) -> {
//            collector.collect(event.user);
//            collector.collect(event.url);
//        }).returns(new TypeHint<Collector<String>>() {
//        });

        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<Event, String> {
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
        }
    }
}
