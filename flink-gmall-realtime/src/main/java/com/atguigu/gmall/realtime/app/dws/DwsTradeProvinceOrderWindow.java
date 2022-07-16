package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.TradeProvinceOrderBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 交易域省份粒度下单各窗口轻度聚合
 *
 * @author pangzl
 * @create 2022-07-16 23:12
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop202:8020/gmall/ck");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 3.从kafka的下单事实表中读取下单数据
        //3.1 声明消费的主题以及消费者组
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.2 消费数据  封装为流
        DataStreamSource<String> kafkaStrDS = env.addSource(kafkaConsumer);

        //TODO 4.对读取的数据进行类型转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //{"create_time":"2022-07-08 09:30:55","sku_num":"1","split_original_amount":"5999.0000",
        // "sku_id":"3","date_id":"2022-07-08","source_type_name":"用户查询","user_id":"105",
        // "province_id":"34","source_type_code":"2401","row_op_ts":"2022-07-15 01:30:56.941Z",
        // "sku_name":"小米10 至尊纪念版 双模5G 骁龙865 120HZ高刷新率  游戏手机",
        // "id":"349","order_id":"175","split_total_amount":"5999.0","ts":"1657848655"}

        //TODO 5.去重
        //5.1 按照唯一键进行分组
        KeyedStream<JSONObject, String> detailKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        //5.2 使用状态 + 定时器实现去重
        SingleOutputStreamOperator<JSONObject> distinctDS = detailKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastJsonObjState
                                = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            //注册定时器
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            lastJsonObjState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastJsonObj.getString("row_op_ts");
                            String curRowOpTS = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, curRowOpTS) <= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            out.collect(lastJsonObj);
                            //清空状态
                            lastJsonObjState.clear();
                        }
                    }
                }
        );

        //TODO 6.再次转换流中数据类型  jsonObj -> 实体类对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> tradeProvinceDS = distinctDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) throws Exception {
                        TradeProvinceOrderBean orderBean = TradeProvinceOrderBean.builder()
                                .provinceId(jsonObj.getString("province_id"))
                                .orderAmount(jsonObj.getDouble("split_total_amount"))
                                .orderIdSet(new HashSet<>(Collections.singleton(jsonObj.getString("order_id"))))
                                .ts(jsonObj.getLong("ts") * 1000)
                                .build();
                        return orderBean;
                    }
                }
        );

        //TODO 7.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<TradeProvinceOrderBean> withWatermarkDS = tradeProvinceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderBean>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

        //TODO 8.按照统计粒度省份id进行分组
        KeyedStream<TradeProvinceOrderBean, String> keyedDS = withWatermarkDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        //TODO 9.开窗
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 10.聚合计算
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) throws Exception {
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) throws Exception {
                        for (TradeProvinceOrderBean orderBean : input) {
                            orderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                            orderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                            orderBean.setTs(System.currentTimeMillis());
                            orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                            out.collect(orderBean);
                        }
                    }
                }
        );

        //TODO 11.和省份维度进行关联 获取省份名称
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceNameDS = AsyncDataStream.unorderedWait(
                reduceDS,

                new DimAsyncFunction<TradeProvinceOrderBean>("dim_base_province") {
                    @Override
                    public void join(TradeProvinceOrderBean orderBean, JSONObject dimInfoJsonObj) {
                        orderBean.setProvinceName(dimInfoJsonObj.getString("NAME"));
                    }

                    @Override
                    public String getKey(TradeProvinceOrderBean orderBean) {
                        return orderBean.getProvinceId();
                    }
                },
                60, TimeUnit.SECONDS
        );

        //TODO 12.将关联结果写到ClickHouse中
        withProvinceNameDS.print(">>>");

        withProvinceNameDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
