package com.atguigu.transform;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Map 转换算子
 *
 * @author pangzl
 * @create 2022-06-18 20:18
 */
public class MapExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // map主要用于将数据流中的数据进行转换形成新的数据流
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        // 1.匿名函数类实现map
        streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        }).print("匿名函数");
        // 2.自定义类实现map
        streamSource.map(new MyMapFunction()).print("自定义类实现");
        // 3.lambda表达式实现map
        streamSource.map((event -> event.user)).print("lambda表示式");
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
