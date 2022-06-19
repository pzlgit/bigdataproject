package com.atguigu.partition;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * rescale 重缩放分区
 *
 * @author pangzl
 * @create 2022-06-19 10:23
 */
public class RescaleTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // rescale 重缩放分区
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 1; i < 10; i++) {
                            if (getRuntimeContext().getIndexOfThisSubtask() == i % 2) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(2)
                .rescale()
                .print().setParallelism(4);
        env.execute();
    }
}
