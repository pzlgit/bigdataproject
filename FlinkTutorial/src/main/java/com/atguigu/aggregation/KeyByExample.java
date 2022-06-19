package com.atguigu.aggregation;

import com.atguigu.bean.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * keyBy 分区操作
 *
 * @author pangzl
 * @create 2022-06-18 20:52
 */
public class KeyByExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Bob", "./cart", 2000L)
        );

        // keyBy通过匿名函数类实现
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).print("匿名函数类");

        // keyBy通过Lambda表达式实现
        stream.keyBy(event -> event.user).print("Lambda表达式实现");
        env.execute();
    }
}
