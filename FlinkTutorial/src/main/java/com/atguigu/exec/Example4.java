package com.atguigu.exec;

import com.atguigu.source.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 检查点设置
 *
 * @author pangzl
 * @create 2022-06-24 11:27
 */
public class Example4 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置每隔10S保存一次检查点
        env.enableCheckpointing(10 * 1000L);

        // 设置检查点路径
        env.setStateBackend(new FsStateBackend("file:///D:\\WorkShop\\BIgDataProject\\output"));
        env.addSource(new ClickSource()).print();
        env.execute();
    }
}
