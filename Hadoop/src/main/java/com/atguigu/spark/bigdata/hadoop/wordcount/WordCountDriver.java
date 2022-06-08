package com.atguigu.spark.bigdata.hadoop.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce WordCount 案例实操
 *
 * @author pangzl
 * @create 2022-04-24 14:29
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 获取配置信息以及获取job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);
        // 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        // 设置Mapper输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 关联最终的输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job执行
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}