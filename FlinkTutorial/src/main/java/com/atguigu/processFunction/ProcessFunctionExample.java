package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 使用 处理函数 process function 实现 flatmap 功能
 *
 * @author pangzl
 * @create 2022-06-21 18:22
 */
public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取输入源
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                ).process(new ProcessFunction<Event, String>() {
                              @Override
                              public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                                  if ("Mary".equals(value.user)) {
                                      out.collect(value.user);
                                  } else {
                                      out.collect(value.user + "-" + value.url);
                                  }
                                  System.out.println(ctx.timerService().currentWatermark());
                              }

                              @Override
                              public void onTimer(long timestamp, ProcessFunction<Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                  // 定时器操作，用于执行定时器开始的操作
                              }
                          }
                ).print();
        env.execute();
    }
}
