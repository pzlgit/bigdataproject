package com.atguigu.partition;

import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * shuffle 随机分区
 *
 * @author pangzl
 * @create 2022-06-19 10:14
 */
public class ShuffleTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // shuffle 随机分区
        env.addSource(new ClickSource())
                .shuffle().print().setParallelism(4);
        env.execute();
    }
}
