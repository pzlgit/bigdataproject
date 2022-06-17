package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 有界流读取文件统计单词出现次数
 *
 * @author pangzl
 * @create 2022-06-15 18:46
 */
public class BoundedStreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.读取文件中的数据
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");
        // 3.将每行数据转化为二元组类型（word,1L）
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne =
                lineDataStreamSource.flatMap((String line, Collector<String> out) -> {
                            Arrays.stream(line.split(" ")).forEach(out::collect);
                        })
                        .returns(Types.STRING)
                        .map(word -> Tuple2.of(word, 1L))
                        .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4.分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(r -> r.f0);
        // 5.求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKS.sum(1);
        // 6.打印结果
        sum.print();
        // 7.执行
        env.execute();
    }
}
