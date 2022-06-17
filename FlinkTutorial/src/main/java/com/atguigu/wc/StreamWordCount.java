package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 无界流处理
 * 监听数据发送端主机的指定端口，统计发送来的文本数据中出现过的单词的个数。
 *
 * @author pangzl
 * @create 2022-06-15 18:58
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 实际生产环境中，一般会对端口号和地址进行配置读取，不会写死
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        // 2.从监听端口7777实时读取数据
        DataStreamSource<String> lineDataStreamSource =
                env.socketTextStream(host, port);
        // 3.将每行数据转化为二元组类型（word,1L）
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne =
                lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    // 切割数据
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4.分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(r -> r.f0);
        // 5.求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKS.sum(1);
        // 6.打印结果
        sum.print().setParallelism(3);
        // 7.执行
        env.execute();
    }
}
