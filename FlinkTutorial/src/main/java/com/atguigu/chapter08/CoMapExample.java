package com.atguigu.chapter08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect 连接流测试
 *
 * @author pangzl
 * @create 2022-06-24 21:00
 */
public class CoMapExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStream<Long> stream2 = env.fromElements(1L, 2L, 3L);

        // 连接流
        stream1.connect(stream2)
                .map(new CoMapFunction<Integer, Long, String>() {
                         @Override
                         public String map1(Integer value) throws Exception {
                             return "1-" + value.toString();
                         }

                         @Override
                         public String map2(Long value) throws Exception {
                             return "2" + value.toString();
                         }
                     }
                ).print();
        env.execute();
    }
}
