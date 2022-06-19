package com.atguigu.source;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 从内存集合或者元素中读取数据
 *
 * @author pangzl
 * @create 2022-06-18 19:18
 */
public class FromCollectionSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合内存中读取数据
        List<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        env.fromCollection(events)
                .print("fromCollection");

        // 从元素中读取数据
        env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        ).print("fromElements");

        env.execute();
    }
}
