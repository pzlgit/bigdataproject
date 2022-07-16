package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 交易域SKU粒度下单各窗口汇总表
 *
 * @author pangzl
 * @create 2022-07-16 22:59
 */
public class DwsTradeSkuOrderWindow {

    public static void main(String[] args) throws Exception {
        // TODO 1. 基本环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // TODO 2. 检查点相关设置(略)

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_spu_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer =
                MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 过滤字段不完整数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        return userId != null && sourceTypeName != null;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream =
                filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id（订单明细ID）分组
        KeyedStream<JSONObject, String> keyedStream =
                mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 去重(状态 + 定时器)
        SingleOutputStreamOperator<JSONObject> processedStream =
                keyedStream.process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            // 保存上次已经来过的订单明细数据
                            private ValueState<JSONObject> lastValueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                lastValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                                );
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx,
                                                       Collector<JSONObject> out) throws Exception {
// 从状态中获取订单明细数据
                                JSONObject lastValue = lastValueState.value();
                                if (lastValue == null) {
// 如果状态的数据为空，则需要定义定时器5s后执行
                                    long currentProcessingTime =
                                            ctx.timerService().currentProcessingTime();
                                    ctx.timerService().registerProcessingTimeTimer(
                                            currentProcessingTime + 5000L);
                                    lastValueState.update(jsonObj);
                                } else {
// 说明当前订单明细数据已经重复了，获取两条数据中的最大时间戳的那个
                                    String lastRowOpTs = lastValue.getString("row_op_ts");
                                    String rowOpTs = jsonObj.getString("row_op_ts");
                                    if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                        lastValueState.update(jsonObj);
                                    }
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx,
                                                Collector<JSONObject> out) throws IOException {
                                JSONObject lastValue = this.lastValueState.value();
                                if (lastValue != null) {
                                    out.collect(lastValue);
                                }
                                lastValueState.clear();
                            }
                        }
                );

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeSkuOrderBean> javaBeanStream =
                processedStream.map(
                        jsonObj -> {
                            String orderId = jsonObj.getString("order_id");
                            String userId = jsonObj.getString("user_id");
                            String skuId = jsonObj.getString("sku_id");
                            Double splitOriginalAmount =
                                    jsonObj.getDouble("split_original_amount");
                            Double splitActivityAmount =
                                    jsonObj.getDouble("split_activity_amount");
                            Double splitCouponAmount =
                                    jsonObj.getDouble("split_coupon_amount");
                            Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                            Long ts = jsonObj.getLong("ts") * 1000L;
                            TradeSkuOrderBean trademarkCategoryUserOrderBean =
                                    TradeSkuOrderBean.builder()
                                            .orderIdSet(new HashSet<String>(
                                                    Collections.singleton(orderId)
                                            ))
                                            .skuId(skuId)
                                            .userId(userId)
                                            .orderUuCount(0L)
                                            .originalAmount(splitOriginalAmount)
                                            .activityAmount(splitActivityAmount == null ? 0.0 : splitActivityAmount)
                                            .couponAmount(splitCouponAmount == null ? 0.0 : splitCouponAmount)
                                            .orderAmount(splitTotalAmount)
                                            .ts(ts)
                                            .build();
                            return trademarkCategoryUserOrderBean;
                        }
                );

        // TODO 8. 按照 user_id 分组
        KeyedStream<TradeSkuOrderBean, String> userIdKeyedStream =
                javaBeanStream.keyBy(TradeSkuOrderBean::getUserId);

        // TODO 9. 统计下单独立用户数（Flink状态编程）
        SingleOutputStreamOperator<TradeSkuOrderBean> withUuCtJavaBean =
                userIdKeyedStream.process(
                        new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {

                            private ValueState<String> lastOrderDt;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                ValueStateDescriptor<String> valueStateDescriptor =
                                        new ValueStateDescriptor<>("sku_window_last_order_dt", String.class);
                                // 设置状态 ttl
                                valueStateDescriptor.enableTimeToLive(
                                        StateTtlConfig
                                                .newBuilder(Time.days(1L))
                                                .updateTtlOnCreateAndWrite()
                                                .build()
                                );
                                lastOrderDt = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(TradeSkuOrderBean tradeSkuOrderBean,
                                                       Context context, Collector<TradeSkuOrderBean> out) throws Exception {
                                Long ts = tradeSkuOrderBean.getTs();
                                String curDate = DateFormatUtil.toDate(ts);
                                String lastDt = lastOrderDt.value();
                                if (lastDt == null || !lastDt.equals(curDate)) {
                                    tradeSkuOrderBean.setOrderUuCount(1L);
                                    lastOrderDt.update(curDate);
                                }
                                out.collect(tradeSkuOrderBean);
                            }
                        }
                );

        // TODO 10. 设置水位线和提取事件时间字段
        SingleOutputStreamOperator<TradeSkuOrderBean> withWatermarkDS =
                withUuCtJavaBean.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TradeSkuOrderBean>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                                            @Override
                                            public long extractTimestamp(TradeSkuOrderBean javaBean, long recordTimestamp) {
                                                return javaBean.getTs();
                                            }
                                        }
                                )
                );

        // TODO 11. 分组（按照统计维度sku_id进行分组）
        KeyedStream<TradeSkuOrderBean, String> keyedForAggregateStream =
                withWatermarkDS.keyBy(
                        new KeySelector<TradeSkuOrderBean, String>() {
                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) throws Exception {
                                return javaBean.getSkuId();
                            }
                        }
                );

        // TODO 12. 开窗(滚动事件时间窗口10s)
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS =
                keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 13. 聚合计算并设置窗口开始时间和结束时间
        SingleOutputStreamOperator<TradeSkuOrderBean> reducedStream = windowDS
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                            TradeSkuOrderBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                value1.setOrderUuCount(value1.getOrderUuCount() + value2.getOrderUuCount());
                                value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                                value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                                value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean,
                                String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) throws Exception {
                                String stt =
                                        DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt =
                                        DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TradeSkuOrderBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setOrderCount((long) (element.getOrderIdSet().size()));
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 14. 维度关联，补充与分组无关的维度字段
        // 14.1 关联sku_info表
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoStream =
                AsyncDataStream.unorderedWait(
                        reducedStream,
                        new DimAsyncFunction<TradeSkuOrderBean>("dim_sku_info".toUpperCase()) {

                            @Override
                            public void join(TradeSkuOrderBean javaBean, JSONObject jsonObj)
                                    throws Exception {
                                javaBean.setSkuName(jsonObj.getString("sku_name".toUpperCase()));
                                javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                                javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                                javaBean.setSpuId(jsonObj.getString("spu_id".toUpperCase()));
                            }

                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) {
                                return javaBean.getSkuId();
                            }
                        },
                        60 * 5, TimeUnit.SECONDS
                );
        // 14.2 关联 spu_info 表
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoStream =
                AsyncDataStream.unorderedWait(
                        withSkuInfoStream,
                        new DimAsyncFunction<TradeSkuOrderBean>("dim_spu_info".toUpperCase()) {
                            @Override
                            public void join(TradeSkuOrderBean javaBean,
                                             JSONObject dimJsonObj) throws Exception {
                                javaBean.setSpuName(
                                        dimJsonObj.getString("spu_name".toUpperCase())
                                );
                            }

                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) {
                                return javaBean.getSpuId();
                            }
                        },
                        60 * 5, TimeUnit.SECONDS
                );

        // 14.3 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeSkuOrderBean> withTrademarkStream =
                AsyncDataStream.unorderedWait(
                        withSpuInfoStream,
                        new DimAsyncFunction<TradeSkuOrderBean>("dim_base_trademark".toUpperCase()) {
                            @Override
                            public void join(TradeSkuOrderBean javaBean,
                                             JSONObject jsonObj) throws Exception {
                                javaBean.setTrademarkName(
                                        jsonObj.getString("tm_name".toUpperCase()));
                            }

                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) {
                                return javaBean.getTrademarkId();
                            }
                        },
                        5 * 60, TimeUnit.SECONDS
                );

        // 14.4 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3Stream =
                AsyncDataStream.unorderedWait(
                        withTrademarkStream,
                        new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category3".toUpperCase()) {
                            @Override
                            public void join(TradeSkuOrderBean javaBean,
                                             JSONObject jsonObj) throws Exception {
                                javaBean.setCategory3Name(
                                        jsonObj.getString("name".toUpperCase()));
                                javaBean.setCategory2Id(
                                        jsonObj.getString("category2_id".toUpperCase()));
                            }

                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) {
                                return javaBean.getCategory3Id();
                            }
                        },
                        5 * 60, TimeUnit.SECONDS
                );

        // 14.5 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2Stream =
                AsyncDataStream.unorderedWait(
                        withCategory3Stream,
                        new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category2".toUpperCase()) {
                            @Override
                            public void join(TradeSkuOrderBean javaBean,
                                             JSONObject jsonObj) throws Exception {
                                javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                                javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                            }

                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) {
                                return javaBean.getCategory2Id();
                            }
                        },
                        5 * 60, TimeUnit.SECONDS
                );

        // 14.6 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1Stream =
                AsyncDataStream.unorderedWait(
                        withCategory2Stream,
                        new DimAsyncFunction<TradeSkuOrderBean>("dim_base_category1".toUpperCase()) {
                            @Override
                            public void join(TradeSkuOrderBean javaBean,
                                             JSONObject jsonObj) throws Exception {
                                javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                            }

                            @Override
                            public String getKey(TradeSkuOrderBean javaBean) {
                                return javaBean.getCategory1Id();
                            }
                        },
                        5 * 60, TimeUnit.SECONDS
                );

        // TODO 15. 写出到OLAP数据库ClickHouse
        SinkFunction<TradeSkuOrderBean> jdbcSink =
                ClickHouseUtil.<TradeSkuOrderBean>getJdbcSink(
                        "insert into dws_trade_sku_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
        withCategory1Stream.<TradeSkuOrderBean>addSink(jdbcSink);

        env.execute();
    }
}
