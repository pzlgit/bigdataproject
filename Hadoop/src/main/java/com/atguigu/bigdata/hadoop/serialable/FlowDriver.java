package com.atguigu.bigdata.hadoop.serialable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author pangzl
 * @create 2022-04-24 15:35
 */
public class FlowDriver {

    public static void main(String[] args) throws Exception {
        // 创建配置对象并获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 绑定本Driver类
        job.setJarByClass(FlowDriver.class);
        // 绑定Mapper和Reduce类
        job.setMapperClass(FlowerMapper.class);
        job.setReducerClass(FlowReduce.class);
        // 指定Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        // 指定最终输出KV的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 指定输入输出地址
        FileInputFormat.setInputPaths(job, new Path("D:\\WorkShop\\BIgDataProject\\Data\\phone.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\WorkShop\\BIgDataProject\\Data\\output2"));
        // 提交任务执行
        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);
    }
}
