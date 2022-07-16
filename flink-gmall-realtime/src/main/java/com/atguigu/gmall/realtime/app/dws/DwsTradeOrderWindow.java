package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradeOrderBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 交易域下单各窗口轻度聚合汇总表
 *
 * @author pangzl
 * @create 2022-07-13 16:52
 */
public class DwsTradeOrderWindow {

    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka dwd_trade_order_detail 读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream =
                mappedStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<JSONObject>() {
                                            @Override
                                            public long extractTimestamp(JSONObject jsonObj,
                                                                         long recordTimestamp) {
                                                return jsonObj.getLong("ts") * 1000;
                                            }
                                        }
                                )
                );

        // TODO 6. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedByUserIdStream =
                withWatermarkStream.keyBy(r -> r.getString("user_id"));

        // TODO 7. 统计当日下单独立用户数和新增下单用户数
        SingleOutputStreamOperator<TradeOrderBean> orderBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastOrderDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("lastOrderDtState", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradeOrderBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();

                        Long ts = jsonObj.getLong("ts") * 1000L;
                        String orderDt = DateFormatUtil.toDate(ts);

                        Long orderNewUserCount = 0L;
                        Long orderUniqueUserCount = 0L;

                        if (lastOrderDt == null) {
                            orderNewUserCount = 1L;
                            orderUniqueUserCount = 1L;
                            lastOrderDtState.update(orderDt);
                        } else {
                            if (!lastOrderDt.equals(orderDt)) {
                                orderUniqueUserCount = 1L;
                                lastOrderDtState.update(orderDt);
                            }
                        }

                        if (orderNewUserCount != 0L || orderUniqueUserCount != 0L) {
                            TradeOrderBean tradeOrderBean = new TradeOrderBean(
                                    "",
                                    "",
                                    orderUniqueUserCount,
                                    orderNewUserCount,
                                    0L
                            );
                            out.collect(tradeOrderBean);
                        }
                    }
                }
        );

        // TODO 8. 开窗
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = orderBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<TradeOrderBean> reducedStream =
                windowDS.reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() +
                                        value2.getOrderNewUserCount());

                                value1.setOrderUniqueUserCount(
                                        value1.getOrderUniqueUserCount() +
                                                value2.getOrderUniqueUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                                String stt =
                                        DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt =
                                        DateFormatUtil.toYmdHms(context.window().getEnd());

                                for (TradeOrderBean value : values) {
                                    value.setStt(stt);
                                    value.setEdt(edt);
                                    value.setTs(System.currentTimeMillis());
                                    out.collect(value);
                                }
                            }
                        }
                );

        // TODO 10. 写出到 OLAP 数据库ClickHouse中
        reducedStream.print(">>");
        reducedStream.<TradeOrderBean>addSink(
                ClickHouseUtil.<TradeOrderBean>getJdbcSink(
                        "insert into dws_trade_order_window values(?,?,?,?,?)"
                ));

        env.execute();
    }

}
