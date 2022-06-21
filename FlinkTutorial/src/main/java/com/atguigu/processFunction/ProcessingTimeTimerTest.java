package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 处理时间定时器
 *
 * @author pangzl
 * @create 2022-06-21 18:30
 */
public class ProcessingTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据源
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        // 定义处理时间定时器,不用指定水位线和时间戳，因为使用的是处理时间
        streamSource.keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        // 输出处理时间
                        out.collect("数据到达，到达时间：" + new Timestamp(currentProcessingTime));
                        // 定义10s后的定时器(处理时间)
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发执行操作
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }
}
