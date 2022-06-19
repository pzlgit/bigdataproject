package com.atguigu.udf;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author pangzl
 * @create 2022-06-18 21:47
 */
public class RichFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> clicks = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=1", 5 * 1000L),
                new Event("Cary", "./home", 60 * 1000L)
        );
        // 将点击事件转换成长整型的时间戳输出
        DataStream<Long> result = clicks.map(new RichMapFunction<Event, Long>() {
            // 每个并行子任务执行一次
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引为：" + getRuntimeContext().getIndexOfThisSubtask() + "的任务开始");
            }

            // 每个并行子任务执行一次
            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为 " + getRuntimeContext().getIndexOfThisSubtask() + " 的任务结束");
            }

            // 每条数据执行一次
            @Override
            public Long map(Event event) throws Exception {
                return event.timestamp;
            }
        });
        result.print();

        env.execute();
    }
}
