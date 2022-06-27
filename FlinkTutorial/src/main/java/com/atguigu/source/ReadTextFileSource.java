package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件中读取数据
 *
 * @author pangzl
 * @create 2022-06-18 19:23
 */
public class ReadTextFileSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件中读取数据
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");
        lineDataStreamSource.print("readTextFile");

        DataStream<String> stream = env.readTextFile("hdfs://hadoop102:9820/company/dept/dept.txt");
        stream.print();
        env.execute();
    }
}
