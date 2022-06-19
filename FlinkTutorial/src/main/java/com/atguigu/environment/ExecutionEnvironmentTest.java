package com.atguigu.environment;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 创建执行环境
 *
 * @author pangzl
 * @create 2022-06-18 19:00
 */
public class ExecutionEnvironmentTest {

    public static void main(String[] args) {

        // 1.能够自动判断执行环境，动态获取执行环境：本地环境和集群环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println(env.getClass());

        // 可以将流处理模式设置为批处理模式，但是一般不推荐在代码中硬编码设置
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 2.创建本地执行环境
        LocalStreamEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();
        System.out.println(localEnv);
        System.out.println(localEnv.getParallelism());

        // 3.创建集群执行环境
        StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment(
                "hadoop102",
                6123,
                "D:\\WorkShop\\BIgDataProject\\FlinkTutorial\\target\\FlinkTutorial-1.0-SNAPSHOT.jar"
        );
        System.out.println(remoteEnv.getClass());
        System.out.println(remoteEnv.getParallelism());

        // 4.创建批处理执行环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        System.out.println(batchEnv.getClass());


    }
}
