package com.atguigu.spark.bigdata.hadoop.serialable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author pangzl
 * @create 2022-04-24 15:35
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        // 统计values中的总的数据值
        long totalUpFlow = 0;
        long totalDownFlow = 0;
        for (FlowBean flowBean : values) {
            totalUpFlow += flowBean.getUpFlow();
            totalDownFlow += flowBean.getDownFlow();
        }
        flowBean.setUpFlow(totalUpFlow);
        flowBean.setDownFlow(totalDownFlow);
        flowBean.setSumFlow(totalDownFlow + totalUpFlow);
        context.write(key, flowBean);
    }
}
