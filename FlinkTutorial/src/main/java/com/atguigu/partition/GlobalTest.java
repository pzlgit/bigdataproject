package com.atguigu.partition;

import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * global 全局分区
 *
 * @author pangzl
 * @create 2022-06-19 10:32
 */
public class GlobalTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // global 全局分区
        env.addSource(new ClickSource())
                .global()
                .print().setParallelism(4);
        env.execute();
    }
}
