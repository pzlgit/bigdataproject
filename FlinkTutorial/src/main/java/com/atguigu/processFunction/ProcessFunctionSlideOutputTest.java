package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 处理函数侧输出流测试
 *
 * @author pangzl
 * @create 2022-06-21 19:20
 */
public class ProcessFunctionSlideOutputTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 定义输出流标签
        OutputTag<String> outputTag = new OutputTag<String>("slide-out") {
        };

        SingleOutputStreamOperator<String> process = env.addSource(new ClickSource())
                .process(
                        new ProcessFunction<Event, String>() {
                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                                // 将url输出到主流中
                                out.collect(value.url);
                                // 将user输出到侧输出流中
                                ctx.output(outputTag, "slide-output" + value.user);
                            }
                        }
                );
        // 侧输出流打印
        process.print("main");
        process.getSideOutput(outputTag).print("slide-output");
        env.execute();
    }

}
