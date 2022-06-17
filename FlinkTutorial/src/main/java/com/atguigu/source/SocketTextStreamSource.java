package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从socket流中读取数据
 *
 * @author pangzl
 * @create 2022-06-17 20:07
 */
public class SocketTextStreamSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从socketTextStream流中读取数据
        env.socketTextStream("hadoop102", 7777)
                .print("socketTextStream");
        env.execute();
    }
}
