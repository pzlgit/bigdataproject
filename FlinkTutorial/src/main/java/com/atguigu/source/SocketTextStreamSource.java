package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从Socket文本流中读取数据
 *
 * @author pangzl
 * @create 2022-06-18 19:26
 */
public class SocketTextStreamSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文本流中读取数据
        env.socketTextStream("hadoop102", 7777)
                .print("socketTextStream");
        env.execute();
    }
}
