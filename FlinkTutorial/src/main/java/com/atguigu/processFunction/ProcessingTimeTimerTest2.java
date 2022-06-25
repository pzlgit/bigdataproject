package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 处理时间定时器 第二刷
 *
 * @author pangzl
 * @create 2022-06-24 19:14
 */
public class ProcessingTimeTimerTest2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从数据源读取数据，无需指定水位线和时间戳，因为这里使用的是处理时间定时器
        env.addSource(new ClickSource())
                .keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        // 每来一条数据就执行一次
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        out.collect("数据到达，到达时间：" + new Timestamp(currentProcessingTime));
                        // 注册一个10s后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发执行
                        out.collect("定时器执行，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }
}
