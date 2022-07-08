package com.atguigu.gmall.realtime.app.dwd;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * DWD 交易域加购事务事实表
 *
 * @author pangzl
 * @create 2022-07-08 9:51
 */
public class DwdTradeCartAdd {

    public static void main(String[] args) {
        // TODO 1.基本环境准备
        // 1.1 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 设定Table中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // TODO 2.检查点相关设置
        // 2.1 开启检查点并设置检查点对齐
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        // 2.3 取消job后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // 2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));
        // 2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:9820/gmall/ck");
        // 2.7 设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3.从Kafka的topic_db主题中获取数据
        tableEnv.executeSql("CREATE TABLE topic_db\n" +
                "(\n" +
                "    `database` string ,\n" +
                "    `table` string,\n" +
                "    `type` string,\n" +
                "    `data` map<string,string>,\n" +
                "    `old` map<string,string>,\n" +
                "    `ts` string,\n" +
                "    `proc_time` AS proctime()\n" +
                " ) WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'topic_db',\n" +
                "      'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "      'properties.group.id' = 'dwd_trade_cart_add',\n" +
                "      'scan.startup.mode' = 'latest-offset',\n" +
                "      'format' = 'json'\n" +
                "      )");

        //tableEnv.executeSql("select * from topic_db").print();

        // TODO 4.从topic_db表中过滤购物车数据，获取购物车动态表
        Table cartAdd = tableEnv.sqlQuery("select data['id']                                                                       id,\n" +
                "       data['user_id']                                                                  user_id,\n" +
                "       data['sku_id']                                                                   sku_id,\n" +
                "       data['source_id']                                                                source_id,\n" +
                "       data['source_type']                                                              source_type,\n" +
                "       if(`type` = 'insert', data['sku_num'],\n" +
                "          cast(cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num,\n" +
                "       ts,\n" +
                "       proc_time\n" +
                "from topic_db\n" +
                "where `table` = 'cart_info'\n" +
                "  and (`type` = 'insert' or (type = 'update' and `old`['sku_num'] is not null and\n" +
                "                             cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        // 注册加购表
        tableEnv.createTemporaryView("cart_add", cartAdd);

        //tableEnv.executeSql("select * from cart_add").print();

        // TODO 5.从MySQL中读取字典表数据
        tableEnv.executeSql("CREATE TABLE base_dic\n" +
                "(\n" +
                "    dic_code     string,\n" +
                "    dic_name     string,\n" +
                "    parent_code  string,\n" +
                "    create_time  timestamp,\n" +
                "    operate_time timestamp,\n" +
                "    PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "    'table-name' = 'base_dic',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'lookup.cache.max-rows' = '500',\n" +
                "    'lookup.cache.ttl' = '1 hour',\n" +
                "    'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")");

        //tableEnv.executeSql("select * from base_dic").print();

        // TODO 6.使用lookup join关联订单表和字典表获得加购明细表
        Table joinTable = tableEnv.sqlQuery("select \n" +
                "  id,\n" +
                "       user_id,\n" +
                "       sku_id,\n" +
                "       source_id,\n" +
                "       source_type,\n" +
                "       dic_name source_type_name,\n" +
                "       sku_num,\n" +
                "       ts\n" +
                "from cart_add c\n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.proc_time as b\n" +
                "on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("join_table", joinTable);

        // TODO 7.建立交易域加购事务事实表dwd_trade_cart_add
        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add\n" +
                "(\n" +
                "    id               string,\n" +
                "    user_id          string,\n" +
                "    sku_id           string,\n" +
                "    source_id        string,\n" +
                "    source_type      string,\n" +
                "    source_type_name string,\n" +
                "    sku_num          string,\n" +
                "    ts               string,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_trade_cart_add',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");

        // TODO 8.将关联后的结果写出到Kafka
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from join_table");
    }
}
