package com.atguigu.chapter08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实时对账需求
 * 可实现一个实时对账的需求，也就是app的支付操作和第三方的支付操作的一个双流Join。
 * App的支付事件和第三方的支付事件将会互相等待5秒钟，
 * 如果等不来对应的支付事件，那么就输出报警信息。
 *
 * @author pangzl
 * @create 2022-06-24 21:06
 */
public class BillCheckExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream =
                env.fromElements(
                        Tuple3.of("order-1", "app", 1000L),
                        Tuple3.of("order-2", "app", 2000L)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple3<String, String, Long>
                                                                                 element, long recordTimestamp) {
                                                return element.f2;
                                            }
                                        })
                );

        // 来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdpartStream =
                env.fromElements(
                        Tuple4.of("order-1", "third-party", "success", 3000L),
                        Tuple4.of("order-3", "third-party", "success", 4000L)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Tuple4<String, String, String, Long>>() {
                                            @Override
                                            public long extractTimestamp(Tuple4<String, String, String,
                                                    Long> element, long recordTimestamp) {
                                                return element.f3;
                                            }
                                        })
                );

        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.connect(thirdpartStream)
                .keyBy(r -> r.f0, r -> r.f0)
                .process(new OrderMatchResult())
                .print();
        env.execute();
    }

    // 自定义实现coProcessFunction
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        // 定义两个状态，用来保存app和第三方的数据
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 获取状态
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>(
                            "app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>(
                            "thirdparty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG)));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 执行定时器，负责对账
            if (appEventState.value() != null) {
                // app不为空，则说明有数据未进行对账
                out.collect("对账失败:" + appEventState.value() + "=>" + "第三方支付平台信息未到");
            }
            if (thirdPartyEventState.value() != null) {
                // 第三方不为空，则说明数据为对账
                out.collect("对账失败：" + thirdPartyEventState + "=>" + "app信息未到");
            }
            // 清空两个列表的状态数据
            appEventState.clear();
            thirdPartyEventState.clear();
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 处理第一条流 app
            if (thirdPartyEventState.value() != null) {
                // 如果第三方流中不为空，说明数据已经到达
                out.collect("对账成功：" + value + "=>" + thirdPartyEventState.value());
                // 清空第三方状态
                thirdPartyEventState.clear();
            } else {
                // 第三方数据还未到
                appEventState.update(value);
                // 注册一个5秒后的定时器，负责对此数据进行对账
                ctx.timerService().registerEventTimeTimer(value.f2 + 5 * 1000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 处理第二条流 第三方
            if (appEventState.value() != null) {
                out.collect("对账成功：" + appEventState.value() + "  " + value);
                // 清空状态
                appEventState.clear();
            } else {
                // 更新状态
                thirdPartyEventState.update(value);
                // 注册一个5秒后的定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3 + 5000L);
            }
        }
    }
}
