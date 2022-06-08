package com.atguigu.spark.bigdata.hadoop.serialable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author pangzl
 * @create 2022-04-24 15:35
 */
public class FlowerMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String line = value.toString();
        // 将数据按照空格切分
        String[] wordArray = line.split("\t");
        // 获取对应的手机号、上行流量，下行流量
        String phoneNumber = wordArray[1];
        Long upFlow = Long.valueOf(wordArray[wordArray.length - 3]);
        Long downFlow = Long.valueOf(wordArray[wordArray.length - 2]);
        // 构建输出
        k.set(phoneNumber);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(upFlow + downFlow);
        context.write(k, flowBean);
    }

}
