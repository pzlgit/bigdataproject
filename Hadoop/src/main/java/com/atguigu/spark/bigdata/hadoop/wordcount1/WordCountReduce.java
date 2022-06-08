package com.atguigu.spark.bigdata.hadoop.wordcount1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 按照MapReduce编程规范，分别编写Mapper，Reducer，Driver。
 *
 * @author pangzl
 * @create 2022-04-24 14:31
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {


    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 遍历values求和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}