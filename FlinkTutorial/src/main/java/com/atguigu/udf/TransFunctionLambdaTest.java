package com.atguigu.udf;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author pangzl
 * @create 2022-06-18 21:30
 */
public class TransFunctionLambdaTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );
        // map函数使用lambda表达式，返回简单类型，不需要进行类型声明
        SingleOutputStreamOperator<String> map = clicks.map(e -> e.url);
        map.print();

        // flatmap函数使用lambda表达式，会出现java的泛型搽除
        DataStream<String> flatMapStream =
                clicks.flatMap((Event event, Collector<String> collector) -> {
                    collector.collect(event.user);
                    collector.collect(event.url);
                }).returns(Types.STRING);
        flatMapStream.print();


        env.execute();
    }
}
