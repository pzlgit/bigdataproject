package com.atguigu.chapter08;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 使用侧输出流进行分流操作
 *
 * @author pangzl
 * @create 2022-06-24 20:37
 */
public class SplitStreamByOutputTag {

    private static OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary") {
    };
    private static OutputTag<String> bobTag = new OutputTag<String>("bobTag") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> process = streamSource.process(
                new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                        if ("Mary".equals(value.user)) {
                            ctx.output(maryTag, Tuple3.of(value.user, value.url, value.timestamp));
                        } else if ("Bob".equals(value.user)) {
                            ctx.output(bobTag, value.toString());
                        } else {
                            // 输出到主流
                            out.collect(value);
                        }
                    }
                }
        );

        process.print("主流");
        process.getSideOutput(maryTag).print("Mary");
        process.getSideOutput(bobTag).print("Bob");

        env.execute();
    }
}
