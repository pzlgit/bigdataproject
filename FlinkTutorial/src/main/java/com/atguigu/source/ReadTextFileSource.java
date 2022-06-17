package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据，一般用于批处理
 *
 * @author pangzl
 * @create 2022-06-17 20:09
 */
public class ReadTextFileSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取数据
        env.readTextFile("input/words.txt").print("readTextFile");
        env.readTextFile("input").print("readTextFile");
        // 从hadoop文件系统中读取文件
        env.readTextFile("hdfs://hadoop102:9820/company/dept/dept.txt").print("hadoop");
        env.execute();
    }
}
