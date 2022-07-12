package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口轻度聚合
 *
 * @author pangzl
 * @create 2022-07-12 11:45
 */
public class DwsTrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // TODO 2.检查点相关设置

        // TODO 3.从页面浏览数据主题中获取数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_ch_ar_is_new_window";
        FlinkKafkaConsumer<String> kafkaConsumer =
                MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);

        SingleOutputStreamOperator<TrafficPageViewBean> mainStream =
                jsonObjStream.map(
                        new MapFunction<JSONObject, TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                                JSONObject common = jsonObj.getJSONObject("common");
                                JSONObject page = jsonObj.getJSONObject("page");

                                // 获取 ts
                                Long ts = jsonObj.getLong("ts");
                                // 获取维度信息
                                String vc = common.getString("vc");
                                String ch = common.getString("ch");
                                String ar = common.getString("ar");
                                String isNew = common.getString("is_new");
                                // 获取页面访问时长
                                Long duringTime = page.getLong("during_time");
                                // 定义变量接受其它度量值
                                Long uvCt = 0L;
                                Long svCt = 0L;
                                Long pvCt = 1L;
                                Long ujCt = 0L;
                                // 判断本页面是否开启了一个新的会话
                                String lastPageId = page.getString("last_page_id");
                                if (lastPageId == null) {
                                    svCt = 1L;
                                }

                                // 封装为实体类
                                TrafficPageViewBean trafficPageViewBean =
                                        new TrafficPageViewBean(
                                                "",
                                                "",
                                                vc,
                                                ch,
                                                ar,
                                                isNew,
                                                uvCt,
                                                svCt,
                                                pvCt,
                                                duringTime,
                                                ujCt,
                                                ts
                                        );
                                return trafficPageViewBean;
                            }
                        }
                );

        // TODO 4.从独立访客数据主题中获取数据，封装为流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer =
                MyKafkaUtil.getKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);

        SingleOutputStreamOperator<TrafficPageViewBean> uvMappedStream =
                uvSource.map(jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");
                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            ts
                    );
                });

        // TODO 5.从用户跳出数据主题中获取数据，封装为流
        // 6.1 从 Kafka dwd_traffic_user_jump_detail 读取跳出明细数据，封装为流
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer =
                MyKafkaUtil.getKafkaConsumer(ujdTopic, groupId);

        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);

        SingleOutputStreamOperator<TrafficPageViewBean> ujdMappedStream =
                ujdSource.map(jsonStr -> {
                    JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts") + 10 * 1000L;

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            0L,
                            0L,
                            0L,
                            0L,
                            1L,
                            ts
                    );
                });

        // TODO 6.合并三条流
        DataStream<TrafficPageViewBean> pageViewBeanDS = mainStream
                .union(ujdMappedStream)
                .union(uvMappedStream);

        // TODO 7.指定水位线和时间戳字段
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = pageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long recordTimestamp) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 8.按照维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedBeanStream = withWatermarkStream.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                        return Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew()
                        );
                    }
                }
        );

        // TODO 9. 开窗（滚动窗口10S,允许10S的延迟关闭窗口）
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream =
                keyedBeanStream.window(TumblingEventTimeWindows.of(
                                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                        .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(3L));

        // TODO 10. 聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reducedStream =
                windowStream.reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void process(Tuple4<String, String, String, String> key, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {

                                String stt =
                                        DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt =
                                        DateFormatUtil.toYmdHms(context.window().getEnd());
                                // 设置窗口信息
                                for (TrafficPageViewBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 11. 将数据写入OLAP数据库ClickHouse中
        reducedStream.addSink(ClickHouseUtil.<TrafficPageViewBean>getJdbcSink(
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute();
    }
}
