package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TradePaymentBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
 * 交易域支付成功轻度聚合汇总表
 *
 * @author pangzl
 * @create 2022-07-13 16:47
 */
public class DwsTradePaymentSucWindow {

    public static void main(String[] args) throws Exception {
// TODO 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 从dwd_trade_pay_detail_suc 主题读取支付成功明细数据，封装为流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        FlinkKafkaConsumer<String> kafkaConsumer =
                MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream =
                source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkSecondStream =
                mappedStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
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

        // TODO 6. 按照用户id分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkSecondStream.keyBy(r -> r.getString("user_id"));

        // TODO 7. 统计独立支付人数和新增支付人数
        SingleOutputStreamOperator<TradePaymentBean> paymentWindowBeanStream =
                keyedByUserIdStream.process(
                        new KeyedProcessFunction<String, JSONObject, TradePaymentBean>() {

                            private ValueState<String> lastPaySucDtState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                lastPaySucDtState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<String>("last_pay_suc_dt_state", String.class)
                                );
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx,
                                                       Collector<TradePaymentBean> out) throws Exception {
                                String lastPaySucDt = lastPaySucDtState.value();
                                Long ts = jsonObj.getLong("ts") * 1000;
                                String paySucDt = DateFormatUtil.toDate(ts);

                                Long paymentSucUniqueUserCount = 0L;
                                Long paymentSucNewUserCount = 0L;

                                if (lastPaySucDt == null) {
                                    paymentSucUniqueUserCount = 1L;
                                    paymentSucNewUserCount = 1L;
                                    lastPaySucDtState.update(paySucDt);
                                } else {
                                    if (!lastPaySucDt.equals(paySucDt)) {
                                        paymentSucUniqueUserCount = 1L;
                                        lastPaySucDtState.update(paySucDt);
                                    }
                                }

                                if (paymentSucUniqueUserCount != 0L || paymentSucNewUserCount != 0L) {
                                    TradePaymentBean tradePaymentWindowBean = new TradePaymentBean(
                                            "",
                                            "",
                                            paymentSucUniqueUserCount,
                                            paymentSucNewUserCount,
                                            ts
                                    );
                                    out.collect(tradePaymentWindowBean);
                                }
                            }
                        }
                );

        // TODO 8. 开窗
        AllWindowedStream<TradePaymentBean, TimeWindow> windowDS = paymentWindowBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<TradePaymentBean> aggregatedDS = windowDS
                .aggregate(
                        new AggregateFunction<TradePaymentBean, TradePaymentBean, TradePaymentBean>() {
                            @Override
                            public TradePaymentBean createAccumulator() {
                                return new TradePaymentBean(
                                        "",
                                        "",
                                        0L,
                                        0L,
                                        0L
                                );
                            }

                            @Override
                            public TradePaymentBean add(TradePaymentBean value, TradePaymentBean accumulator) {
                                accumulator.setPaymentSucUniqueUserCount(
                                        accumulator.getPaymentSucUniqueUserCount() + value.getPaymentSucUniqueUserCount()
                                );
                                accumulator.setPaymentSucNewUserCount(
                                        accumulator.getPaymentSucNewUserCount() + value.getPaymentSucNewUserCount()
                                );
                                return accumulator;
                            }

                            @Override
                            public TradePaymentBean getResult(TradePaymentBean accumulator) {
                                return accumulator;
                            }

                            @Override
                            public TradePaymentBean merge(TradePaymentBean a, TradePaymentBean b) {
                                return null;
                            }
                        },
                        new ProcessAllWindowFunction<TradePaymentBean, TradePaymentBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<TradePaymentBean> elements, Collector<TradePaymentBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TradePaymentBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 10. 写出到OLAP数据库ClickHouse
        aggregatedDS.addSink(ClickHouseUtil.<TradePaymentBean>getJdbcSink(
                "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
        ));

        env.execute();
    }
}
