package com.atguigu.chapter09;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 计算每一个url在每一个窗口中的pv数据
 *
 * @author pangzl
 * @create 2022-06-25 20:33
 */
public class FakeWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream =
                env.addSource(new ClickSource())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Event>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<Event>() {
                                                    @Override
                                                    public long extractTimestamp(Event element,
                                                                                 long recordTimestamp) {
                                                        return element.timestamp;
                                                    }
                                                })
                        );

        // 统计每10s窗口内，每个url的pv
        stream.keyBy(r -> r.url)
                .process(
                        new FakeWindowResult(10000L)
                ).print();
        env.execute();
    }

    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {

        // 定义属性，窗口长度
        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 定义状态，用mapstate保存每个窗口的pv值
        private MapState<Long, Long> windowPvState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取mapstate状态句柄
            windowPvState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>(
                            "window-pv",
                            Long.class,
                            Long.class
                    )
            );
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 每来一条数据，就判断时间戳属于那个窗口
            Long windowStart = value.timestamp / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;

            // 注册windowEnd +  1ms 的定时器，用于触发窗口计算
            ctx.timerService().registerEventTimeTimer(
                    windowEnd - 1
            );

            // 更新状态的pv值
            if (windowPvState.contains(windowStart)) {
                Long pv = windowPvState.get(windowStart);
                windowPvState.put(windowStart, pv + 1);
            } else {
                windowPvState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发执行
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long pv = windowPvState.get(windowStart);
            out.collect("url: " + ctx.getCurrentKey()
                    + " 访问量: " + pv
                    + " 窗口：" + new Timestamp(windowStart) + " ~ " +
                    new Timestamp(windowEnd));
            // 模拟窗口的销毁，清除map中的key
            windowPvState.remove(windowStart);
        }
    }
}
