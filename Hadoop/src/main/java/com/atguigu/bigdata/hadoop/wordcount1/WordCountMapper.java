package com.atguigu.bigdata.hadoop.wordcount1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 按照MapReduce编程规范，分别编写Mapper，Reducer，Driver。
 *
 * @author pangzl
 * @create 2022-04-24 14:31
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每行数据，将每行数据转化为字符串
        String line = value.toString();
        // 将每行数据按照空格切分成数组
        String[] wordList = line.split(" ");
        // 将数组中的每条数据转化为k,v键值对形式写出
        for (String word : wordList) {
            k.set(word);
            context.write(k, v);
        }
    }

}
