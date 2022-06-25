package com.atguigu.processFunction;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 自定义输出流测试 第二刷
 *
 * @author pangzl
 * @create 2022-06-24 20:10
 */
public class SideOutputTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6);

        OutputTag<String> outputTag = new OutputTag<String>("slide-output"){};

        SingleOutputStreamOperator<String> process = streamSource.process(
                new ProcessFunction<Integer, String>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        // 输出到主流
                        out.collect(value.toString());
                        // 输出到侧输出流
                        ctx.output(outputTag, "output-" + value.toString());
                    }
                }
        );

        process.getSideOutput(outputTag).print("slide");
        process.print("main");

        // 获取侧输出流中的数据输出


        env.execute();
    }
}
