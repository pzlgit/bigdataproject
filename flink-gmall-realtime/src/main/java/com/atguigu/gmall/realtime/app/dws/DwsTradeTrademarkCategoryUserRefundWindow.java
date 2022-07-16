package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 交易域品牌-品类-用户粒度退单各窗口 轻度聚合
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        // TODO 2. 检查点相关设置(；略)
        // TODO 3. 从 Kafka dwd_trade_order_refund 主题读取退单明细数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> javaBeanStream = mappedStream.map(
                jsonObj -> {
                    String orderId = jsonObj.getString("order_id");
                    String userId = jsonObj.getString("user_id");
                    String skuId = jsonObj.getString("sku_id");
                    Long ts = jsonObj.getLong("ts") * 1000L;
                    TradeTrademarkCategoryUserRefundBean trademarkCategoryUserOrderBean = TradeTrademarkCategoryUserRefundBean.builder()
                            .orderIdSet(new HashSet<String>(
                                    Collections.singleton(orderId)
                            ))
                            .userId(userId)
                            .skuId(skuId)
                            .ts(ts)
                            .build();
                    return trademarkCategoryUserOrderBean;
                }
        );

        // TODO 8. 维度关联，补充与分组相关的维度字段
        // 关联 sku_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                javaBeanStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_sku_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withWatermarkDS = withSkuInfoStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefundBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 10. 分组
        KeyedStream<TradeTrademarkCategoryUserRefundBean, String> keyedForAggregateStream = withWatermarkDS.keyBy(
                new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) throws Exception {
                        return javaBean.getTrademarkId() +
                                javaBean.getCategory3Id() +
                                javaBean.getUserId();
                    }
                }
        );

        // TODO 11. 开窗
        WindowedStream<TradeTrademarkCategoryUserRefundBean, String, TimeWindow> windowDS = keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 12. 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TradeTrademarkCategoryUserRefundBean> elements, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                        for (TradeTrademarkCategoryUserRefundBean element : elements) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setRefundCount((long) (element.getOrderIdSet().size()));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 13. 维度关联，补充与分组无关的维度字段
        // 13.1 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_trademark".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 13.2 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 13.3 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 13.4 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // TODO 14. 写出到 OLAP 数据库
        SinkFunction<TradeTrademarkCategoryUserRefundBean> jdbcSink =
                ClickHouseUtil.<TradeTrademarkCategoryUserRefundBean>getJdbcSink(
                        "insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
        withCategory1Stream.<TradeTrademarkCategoryUserRefundBean>addSink(jdbcSink);
        env.execute();
    }
}