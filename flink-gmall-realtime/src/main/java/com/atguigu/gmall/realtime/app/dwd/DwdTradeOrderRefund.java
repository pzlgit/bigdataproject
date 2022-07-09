package com.atguigu.gmall.realtime.app.dwd;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 交易域退单事务事实表
 *
 * @author pangzl
 * @create 2022-07-09 16:07
 */
public class DwdTradeOrderRefund {

    public static void main(String[] args) {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 设置状态的TTL失效时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // TODO 2.检查点相关设置（略）

        // TODO 3.从Kafka的topic_db主题中读取业务数据，生成动态表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_order_refund"));

        // TODO 4.读取退单表数据（where table = ''order_refund_info and type ='insert' ）
        Table orderRefundInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['refund_type'] refund_type,\n" +
                "data['refund_num'] refund_num,\n" +
                "data['refund_amount'] refund_amount,\n" +
                "data['refund_reason_type'] refund_reason_type,\n" +
                "data['refund_reason_txt'] refund_reason_txt,\n" +
                "data['create_time'] create_time,\n" +
                "proc_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'order_refund_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 5.读取订单表数据，筛选退单数据（where table = 'order_info' and type =update and data[order_status]=1005 and old[order_status] is not null）
        Table orderInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "`old`\n" +
                "from topic_db\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and data['order_status']='1005'\n" +
                "and `old`['order_status'] is not null");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 6.读取MySQL配置字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 7.关联三张表（退单表、订单表、字典表）获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "ri.id,\n" +
                "ri.user_id,\n" +
                "ri.order_id,\n" +
                "ri.sku_id,\n" +
                "oi.province_id,\n" +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id,\n" +
                "ri.create_time,\n" +
                "ri.refund_type,\n" +
                "type_dic.dic_name,\n" +
                "ri.refund_reason_type,\n" +
                "reason_dic.dic_name,\n" +
                "ri.refund_reason_txt,\n" +
                "ri.refund_num,\n" +
                "ri.refund_amount,\n" +
                "ri.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_refund_info ri\n" +
                "join \n" +
                "order_info oi\n" +
                "on ri.order_id = oi.id\n" +
                "join \n" +
                "base_dic for system_time as of ri.proc_time as type_dic\n" +
                "on ri.refund_type = type_dic.dic_code\n" +
                "join\n" +
                "base_dic for system_time as of ri.proc_time as reason_dic\n" +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8.创建退单表，和Kafka映射
        tableEnv.executeSql("create table dwd_trade_order_refund(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "refund_type_code string,\n" +
                "refund_type_name string,\n" +
                "refund_reason_type_code string,\n" +
                "refund_reason_type_name string,\n" +
                "refund_reason_txt string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_refund"));

        // TODO 9.将关联结果写入到Kafka
        tableEnv.executeSql("insert into dwd_trade_order_refund select * from result_table");
    }
}
