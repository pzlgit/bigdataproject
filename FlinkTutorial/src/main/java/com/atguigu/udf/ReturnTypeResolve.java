package com.atguigu.udf;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author pangzl
 * @create 2022-06-18 21:38
 */
public class ReturnTypeResolve {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        // 1.显示的指定返回值类型
        SingleOutputStreamOperator<Tuple2<String, Long>> streamOperator =
                clicks.map(event -> {
                    return Tuple2.of(event.user, 1L);
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        streamOperator.print();
        // 2.使用类代替Lambda表达式
        clicks.map(new MyMapFunction()).print();
        // 3.使用匿名类来代替Lambda表达式
        clicks.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        }).print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Event, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(Event event) throws Exception {
            return Tuple2.of(event.user, 1L);
        }
    }
}
