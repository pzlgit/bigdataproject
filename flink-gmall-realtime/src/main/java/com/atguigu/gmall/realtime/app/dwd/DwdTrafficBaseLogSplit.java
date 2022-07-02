package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 流量域-日志数据分流
 *
 * @author pangzl
 * @create 2022-07-02 19:01
 */
public class DwdTrafficBaseLogSplit {

    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        // 1.1 指定流环境环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 指定全局并行度，全局并行度与Kafka分区数一致
        env.setParallelism(4);

        // TODO 2.检查点相关设置
        // 2.1 开启检查点，并设置检查点对齐
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        // 2.3 设置Job取消后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置重启策略(失败率错误重启)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 2.6 设置状态后端,并将检查点存储在分布式文件系统HDFS
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:9820/gmall/checkpoint");
        // 2.设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.从Kafka的topic_log主题中获取数据
        // 3.1 声明消费的消费者组和主题
        String topic = "topic_log";
        String groupId = "dwd_traffic_base_log_group";
        // 3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        // 3.3 消费数据，转化为流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        /**
         * 测试是否能消费到Kafka数据
         * 需要启动的进程： zk,kafka,flume,DwdTrafficBaseLogSplit
         * 执行流程：
         *   模拟生成日志数据jar
         *   将生成的日志数据落盘
         *   flume从磁盘采集日志到Kafka的topic_log主题中
         *   DwdTrafficBaseLogSplit从topic_log主题中获取日志数据
         *      -- ETL,将脏数据写出到Kafka主题
         *      -- 对新老访客标记进行修复 -- 状态编程
         *      -- 分流 -- 侧输出流
         */
        // kafkaDS.print(">>>");

        // TODO 4.对读取的数据进行类型转化及ETL(判断是否是JSON格式)
        // 4.1 声明侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        // 4.2 转换结构，将脏数据放到侧输出流中
        SingleOutputStreamOperator<JSONObject> cleanedDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            // 如果能够转化为JSON，说明是标准格式
                            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            // 转化异常，说明不是标准的JSON格式，将数据放到侧输出流中
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        // 4.3 将侧输出流中的脏数据写到Kafka主题中
        DataStream<String> dirtyDS = cleanedDS.getSideOutput(dirtyTag);
        /**
         * 脏数据输出到Kafka中测试
         */
        dirtyDS.print(">>>");
        dirtyDS.addSink(MyKafkaUtil.getKafkaProducer("dirty_data"));

        // TODO 5.对新老访客标记进行修复
        // 5.1 按照mid分组
        KeyedStream<JSONObject, String> keyedDS =
                cleanedDS.keyBy(r -> r.getJSONObject("common").getString("mid"));
        // 5.2 使用Flink的状态变成修复新老访客标记
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    // 声明键控状态ValueState存放mid的首次访问时间
                    // TODO 不能在声明的状态的时候直接对其进行初始化工作，因为这个时候还没有获取到运行时上下文对象
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("lastVisitDateState", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");
                        // 获取状态中的mid的首次访问时间
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.toDate(ts);
                        // 判断是否是新老用户
                        if ("1".equals(isNew)) {
                            // 前端标记为新访客
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                // 如果状态日期为空，说明当前设备以前从来没有访问过，将这次访问时间更新到状态中即可
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                // 如果状态日期不为空，则判断当前访问日志是否和状态中的日期相同
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    // 如果当前访问日期和状态中的日期不相等，说明在今天之前这个设备已经访问过，我们将其修复为0
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }

                        } else {
                            // 前端标记为老访客,判断状态中是否存在上次访问日期，如果不存在，说明日志第一次进入数仓，我们需要在状态中添加一个日期
                            if (StringUtils.isEmpty(lastVisitDate)) {
                                String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterday);
                            }
                        }
                        out.collect(jsonObj);
                    }
                }
        );
        fixedDS.print(">>>");

        // TODO 6.按照日志类型分流--输出流 错误测输出流、启动测输出流、曝光侧输出流、动作侧输出流、页面主流
        // 6.1 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        // 6.2 分流
        SingleOutputStreamOperator<String> pageLogDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                // 处理错误日志
                JSONObject errJsonObj = jsonObj.getJSONObject("err");
                if (errJsonObj != null) {
                    // 将错误日志输出到错误侧输出流
                    ctx.output(errTag, errJsonObj.toJSONString());
                    // 删除掉error属性
                    jsonObj.remove("err");
                }
                // 处理启动日志
                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                if (startJsonObj != null) {
                    ctx.output(startTag, jsonObj.toJSONString());
                } else {
                    // 页面日志处理
                    JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    Long ts = jsonObj.getLong("ts");

                    // 曝光日志处理
                    JSONArray displaysArr = jsonObj.getJSONArray("displays");
                    if (displaysArr != null) {
                        for (int i = 0; i < displaysArr.size(); i++) {
                            JSONObject displayJsonObj = displaysArr.getJSONObject(i);
                            JSONObject returnDisplayJsonObj = new JSONObject();
                            returnDisplayJsonObj.put("common", commonJsonObj);
                            returnDisplayJsonObj.put("page", pageJsonObj);
                            returnDisplayJsonObj.put("ts", ts);
                            returnDisplayJsonObj.put("display", displayJsonObj);
                            ctx.output(displayTag, returnDisplayJsonObj.toJSONString());
                        }
                    }

                    // 动作日志处理
                    JSONArray actionsArr = jsonObj.getJSONArray("actions");
                    if (actionsArr != null) {
                        for (int i = 0; i < actionsArr.size(); i++) {
                            JSONObject actionJsonObj = actionsArr.getJSONObject(i);
                            JSONObject returnActionJsonObj = new JSONObject();
                            returnActionJsonObj.put("common", commonJsonObj);
                            returnActionJsonObj.put("page", pageJsonObj);
                            returnActionJsonObj.put("action", actionJsonObj);
                            ctx.output(actionTag, returnActionJsonObj.toJSONString());
                        }
                        jsonObj.remove("displays");
                        jsonObj.remove("actions");
                        out.collect(jsonObj.toJSONString());
                    }
                }
            }
        });
        DataStream<String> errDS = pageLogDS.getSideOutput(errTag);
        DataStream<String> startDS = pageLogDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageLogDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageLogDS.getSideOutput(actionTag);

        // TODO 7.将不同流的数据写到Kafka的不同主题中
        pageLogDS.print(">>>");
        errDS.print("###");
        startDS.print("$$$");
        displayDS.print("@@");
        actionDS.print("&&&");

        pageLogDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_page_log"));
        errDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_err_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_display_log"));
        actionDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_traffic_action_log"));

        env.execute();
    }

}