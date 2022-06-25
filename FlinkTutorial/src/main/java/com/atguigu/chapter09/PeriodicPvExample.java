package com.atguigu.chapter09;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 根据用户id分组，统计每个用户的pv
 *
 * @author pangzl
 * @create 2022-06-25 20:12
 */
public class PeriodicPvExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 自定义数据源中获取数据并指定水位线和时间戳
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );
        stream.print("input");

        // 统计每个用户的pv,隔一段时间10s输出计算一次结果
        stream.keyBy(r -> r.user)
                .process(new PeriodicPvResult())
                .print();
        env.execute();
    }

    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {

        // 保存用户的pv数据值
        private ValueState<Long> countState;
        // 保存执行的定时器时间戳
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取定义的状态列表数据
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("pv-count", Types.LONG)
            );
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器执行，负责收集数据，并清空定时器状态和pv数据值状态
            out.collect(ctx.getCurrentKey() + " pv：" + countState.value());
            timerTsState.clear();
        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            // 数据每来一次就调用一次，将数据缓存到状态中
            if (countState.value() != null) {
                countState.update(countState.value() + 1);
            } else {
                countState.update(1L);
            }
            // 注册一个定时器，10s后执行,查看状态中是否有定时器时间，如果有那就不注册
            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(
                        value.timestamp + 10 * 1000L
                );
                timerTsState.update(value.timestamp + 10 * 1000L);
            }
        }
    }
}
