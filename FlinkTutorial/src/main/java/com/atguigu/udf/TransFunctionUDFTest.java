package com.atguigu.udf;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author pangzl
 * @create 2022-06-18 21:26
 */
public class TransFunctionUDFTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        clicks.filter(new MyFilterFunction("home")).print();
        clicks.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.url.contains("home");
            }
        }).print();
        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<Event> {
        private String content;

        public MyFilterFunction(String content) {
            this.content = content;
        }

        @Override
        public boolean filter(Event event) throws Exception {
            return event.url.contains(content);
        }
    }
}
