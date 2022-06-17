package com.atguigu.source;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * 从集合或者从元素中读取数据，一般从内存中读取数据
 *
 * @author pangzl
 * @create 2022-06-17 20:01
 */
public class CollectionSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从内存集合中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Bob", "./cart", 2000L));
        env.fromCollection(events).print("fromCollection");

        // 2.从元素中读取数据
        env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        ).print("fromElements");

        env.execute();
    }
}
