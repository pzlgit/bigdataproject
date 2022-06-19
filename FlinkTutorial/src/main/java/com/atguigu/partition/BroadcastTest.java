package com.atguigu.partition;

import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * broadcast 广播分区
 *
 * @author pangzl
 * @create 2022-06-19 10:29
 */
public class BroadcastTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // broadcast 广播分区
        env.addSource(new ClickSource())
                .broadcast()
                .print().setParallelism(4);
        env.execute();
    }
}
