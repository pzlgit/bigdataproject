package com.atguigu.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合使用
 *
 * @author pangzl
 * @create 2022-06-18 20:58
 */
public class TupleAggregationExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("b", 4),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3)
        );

//        stream.keyBy(r -> r.f0).sum("f1").print();
//        stream.keyBy(r -> r.f0).sum(1).print();

//        stream.keyBy(r -> r.f0).max("f1").print();
        stream.keyBy(r -> r.f0).maxBy("f1").print();
        env.execute();
    }
}
