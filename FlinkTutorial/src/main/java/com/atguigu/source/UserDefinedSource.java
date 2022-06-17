package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 用户自定义数据源操作
 *
 * @author pangzl
 * @create 2022-06-17 20:43
 */
public class UserDefinedSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.setParallelism(3);
        // 从自定义数据源中读取数据
        env.addSource(new ClickSource()).setParallelism(2).print("clickSource");
        // 并行度只能设置为1，否则会抛出异常The parallelism of non parallel operator must be 1.

        // 读取并行数据源
        // env.addSource(new CustomSource()).setParallelism(2).print("customSource");
        env.execute();
    }

    // 自定义并行数据源
    public static class CustomSource implements ParallelSourceFunction<Integer> {

        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
