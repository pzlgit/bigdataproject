package com.atguigu.cep;

import com.atguigu.bean.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 超时订单检测
 *
 * @author pangzl
 * @create 2022-06-25 10:21
 */
public class TimeOutOrderTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 自定义数据源获取数据
        DataStreamSource<Event> streamSource = env.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                ctx.collectWithTimestamp(new Event("order-1", "create", 1000L), 1000L);
                ctx.collectWithTimestamp(new Event("order-2", "create", 2000L), 2000L);
                ctx.collectWithTimestamp(new Event("order-1", "pay", 3000L), 3000L);
            }

            @Override
            public void cancel() {

            }
        });

        // 定义pattern
        Pattern<Event, Event> pattern = Pattern.<Event>begin("create-order")
                .where(new SimpleCondition<Event>() {
                           @Override
                           public boolean filter(Event value) throws Exception {
                               return value.url.equals("create");
                           }
                       }
                ).next("pay-order")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.url.equals("pay");
                    }
                }).within(Time.seconds(5));
        // 数据流中执行pattern
        SingleOutputStreamOperator<String> result = CEP.pattern(streamSource.keyBy(r -> r.user), pattern)
                .flatSelect(
                        // 接收订单超时信息的测输出流
                        new OutputTag<String>("time-order") {
                        },
                        // 处理超时订单
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> map, long l, Collector<String> collector) throws Exception {
                                // 将超时信息发送到侧输出流
                                Event event = map.get("create-order").get(0);
                                collector.collect(event.user + "超时未支付");
                            }
                        },
                        // 处理正常支付的订单信息
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                                Event create = map.get("create-order").get(0);
                                Event pay = map.get("pay-order").get(0);
                                collector.collect(create.user + "在" + pay.timestamp + " 进行了支付");
                            }
                        }
                );
        // 将不正常支付的数据输出到测输出流中
        result.getSideOutput(new OutputTag<String>("time-order") {
        }).print("time-order");
        result.print("pay-order");
        env.execute();
    }
}
