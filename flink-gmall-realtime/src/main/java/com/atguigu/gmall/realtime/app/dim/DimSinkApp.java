package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimSinkFunction;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * DIM层维度表读取主程序
 *
 * @author pangzl
 * @create 2022-06-30 9:57
 */
public class DimSinkApp {

    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置全局并行度，Flink全局并行度设置和Kafka分区数一致
        env.setParallelism(4);

        // TODO 2.检查点相关设置
        // 2.1 开启检查点，并设置检查点对齐（EXACTLY_ONCE）
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置检查点超时时间,超时没完成就会被丢弃掉
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        // 2.3 取消Job后是否保留检查点,作业取消后，检查点依然保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置重启策略（失败错误率策略）(30天内只能重启3次，3秒重启一次)
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 2.6 设置状态后端（保存在分布式文件系统HDFS）
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:9820/gmall/checkpoint");
        // 2.7 设置指定操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.从Kafka的topic_db主题中读取业务数据
        // 3.1 声明消费的主题及消费者组
        String topic = "topic_db";
        String groupId = "dim_app_group";
        // 3.2 创建FlinkKafkaConsumer消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        // 3.3 从Kafka消费业务数据
        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        // TODO 4.对业务数据格式转换（JsonString->JsonObject）
        // 4.1 匿名内部类方式实现
        SingleOutputStreamOperator<JSONObject> jsonDS1 = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });
        // 4.2 Lambda表达式方式实现
        SingleOutputStreamOperator<JSONObject> jsonDS2 = kafkaDS.map(message -> JSONObject.parseObject(message));
        // 4.3 使用方法的默认调用实现
        SingleOutputStreamOperator<JSONObject> jsonDS3 = kafkaDS.map(JSONObject::parseObject);

        // TODO 5.对读取的业务数据ETL(过滤全量同步中的无效数据)
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS1.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                try {
                    // 获取data数据，如果json格式异常会报错,那就把这条数据过滤
                    jsonObject.get("data");
                    // 将全量表同步的type=bootstrap-start和bootstrap-complete的数据过滤
                    if (jsonObject.getString("type").equals("bootstrap-start")
                            || jsonObject.getString("type").equals("bootstrap-complete")) {
                        return false;
                    }
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        });

        /**
         * 测试流程：
         *  需要启动的进程：zk,kafka,maxwell,hdfs,DimSinkApp
         *  执行流程：
         *  1. 运行模拟生成业务数据jar
         *  2. 生成的业务数据保存到MySQL
         *  3. Binlog日志实时记录日志变更记录
         *  4. Maxwell读取日志将业务数据封装成json格式发送给kafka
         *  5. DimSinkApp读取topic_db主题中的业务数据进行处理输出
         *
         *  TODO 会出现一个maxwell的问题？
         *  maxwell中增加新的监听的数据库时，没法将数据存储到maxwell对应的原始数据库中，导致新监听的数据库没法找到。
         *  就抛出异常不执行了，（因为maxwell只有在第一次执行的时候才会把数据初始化到内部表中。后续不会新增，如果
         *  要解决这个问题，那就需要将maxwell的原始数据表删除）
         */
        //filterDS.print(">>>");

        // TODO 6.Fink CDC读取配置表数据-配置流
        // 6.1 Flink CDC 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        // 6.2 读取数据
        DataStreamSource<String> mySqlDS = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL Source"
        );

        /**
         * Flink CDC 读取配置流测试
         * 刚开始启动应用程序后，会查询一次table_process表所有的历史数据，
         * 后续开始监听表的变化,持续输出数据
         */
        mySqlDS.print("+MySQL配置表+");

        // TODO 7.将配置流广播-广播流
        MapStateDescriptor<String, TableProcess> state = new MapStateDescriptor<>(
                "table_process_state",
                String.class,
                TableProcess.class
        );
        BroadcastStream<String> broadcastDS = mySqlDS.broadcast(state);

        // TODO 8.将业务主流和配置广播流关联-connect
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);

        // TODO 9.对关联后的数据处理-process
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(new TableProcessFunction(state));

        dimDS.print(">>>");

        // TODO 10.将维度表数据写入到Phoenix表中
        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}