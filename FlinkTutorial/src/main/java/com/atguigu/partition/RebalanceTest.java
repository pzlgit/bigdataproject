package com.atguigu.partition;

import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * rebalance 轮询分区
 *
 * @author pangzl
 * @create 2022-06-19 10:20
 */
public class RebalanceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // rabalance 轮询分区
        env.addSource(new ClickSource())
                .rebalance()
                .print().setParallelism(4);
        env.execute();
    }
}
