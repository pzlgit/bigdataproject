package com.atguigu.aggregation;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author pangzl
 * @create 2022-06-18 21:04
 */
public class TransPojoAggregationTest {

    public static void main(String[] args) throws Exception {
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        stream.keyBy(e -> e.user).max("timestamp").print();
        env.execute();
    }
}
