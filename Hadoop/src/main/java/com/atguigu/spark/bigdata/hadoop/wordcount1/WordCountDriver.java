package com.atguigu.spark.bigdata.hadoop.wordcount1;


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
        // 设置HDFS NameNode地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
        // 指定MapperReduce运行在Yarn上
        configuration.set("mapreduce.framework.name", "yarn");
        // 指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        // 指定Yarn ResourceManager 的位置
        configuration.set("yarn.resourcemanager.hostname", "hadoop103");
        Job job = Job.getInstance(configuration);

        // 关联本Driver程序的jar
        // job.setJarByClass(WordCountDriver.class);
        // 远程向集群提交任务，因为集群上没有自己写的代码，因此要设置下jar包
        job.setJar("D:\\WorkShop\\BIgDataProject\\Hadoop\\target\\Hadoop-1.0-SNAPSHOT.jar");
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