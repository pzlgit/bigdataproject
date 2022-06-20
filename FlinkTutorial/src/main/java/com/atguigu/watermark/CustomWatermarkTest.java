package com.atguigu.watermark;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 周期性水位线生成器
 *
 * @author pangzl
 * @create 2022-06-20 14:54
 */
public class CustomWatermarkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        new CustomWatermarkStrategy()
                ).print();
        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context
        ) {
            return new CustomPunctuatedGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(
                TimestampAssignerSupplier.Context context
        ) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                }
            };
        }
    }

    // 周期性水位线生成器
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {

        // 定义延迟时间和最大时间戳
        private Long delayTime = 5000L;
        private Long maxTs = Long.MIN_VALUE + delayTime + 1;

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            // 数据每来一次，调用一次,更新最大时间戳
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 框架系统周期200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }

    // 断点式水位线生成器
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            if (event.user.equals("Mary")) {
                output.emitWatermark(new Watermark(event.timestamp - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 什么也不做
        }
    }
}
