package com.atguigu.transform;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Filter 过滤
 *
 * @author pangzl
 * @create 2022-06-18 20:25
 */
public class FilterExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        // 1.匿名函数类实现Filter
        stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return "Mary".equals(event.user);
            }
        }).print("匿名函数");
        // 2.自定义类实现Filter
        stream.filter(new MyFilterFunction()).print("自定义类实现");
        // 3.Lambda表达式实现Filter
        stream.filter(event -> "Mary".equals(event.user)).print("Lambda实现");
        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return "Mary".equals(event.user);
        }
    }
}
