package com.atguigu.wc;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理WordCount统计
 *
 * @author pangzl
 * @create 2022-06-15 18:31
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.读取文件（按行读取文件）
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        // 3.转化数据格式为二元组类型（world,1L）
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne =
                lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                            // 按照空格切分数据
                            String[] words = line.split(" ");
                            // 遍历数据输出
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        })
                        // 当Lambda表达式使用Java泛型时，由于泛型搽除的存在，需要显示的声明类型信息
                       // .returns(Types.TUPLE(Types.STRING, Types.LONG));
                        .returns(new TypeHint<Tuple2<String, Long>>() {});
        // 4.分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
        // 5.求和
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);
        // 6.打印
        sum.print();
    }
}
