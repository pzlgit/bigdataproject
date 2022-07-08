package com.atguigu.gmall.realtime.app.dwd;

import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import com.atguigu.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * DWD 交易域订单预处理表
 *
 * @author pangzl
 * @create 2022-07-08 19:04
 */
public class DwdTradeOrderPreProcess {

    public static void main(String[] args) {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 指定表处理环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 设置状态的ttl存活时间，注意：900s 是为了取消订单时，只会修改order_info表数据，如果不将其它数据保留，那么就会导致关联不上的问题。
        // 5s是为了解决不同表数据在传输过程中，可能出现数据乱序的情况，导致数据关联不上的问题。
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(900 + 5));

        // TODO 2.检查点相关设置（略）

        // TODO 3.从Kafka主题topic_db中读取业务数据，封装为动态表
        tableEnv.executeSql("CREATE TABLE topic_db\n" +
                "(\n" +
                "    `database` string,\n" +
                "    `table`    string,\n" +
                "    `type`     string,\n" +
                "    `data`     map<string,string>,\n" +
                "    `old`      map<string,string>,\n" +
                "    `ts`       string,\n" +
                "    `proc_time` AS proctime()\n" +
                ") " + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_pre_process"));

        // TODO 4.读取订单明细表数据 where table = 'order_detail' and type = 'insert'
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['sku_name'] sku_name,\n" +
                "data['create_time'] create_time,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "data['sku_num'] sku_num,\n" +
                "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                "cast(data['order_price'] as decimal(16,2)) as String) split_original_amount,\n" +
                "data['split_total_amount'] split_total_amount,\n" +
                "data['split_activity_amount'] split_activity_amount,\n" +
                "data['split_coupon_amount'] split_coupon_amount,\n" +
                "ts od_ts,\n" +
                "proc_time\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);

        // TODO 5.读取订单表数据 where table = 'order_info' and type = 'insert' or typr = 'update'
        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['province_id'] province_id,\n" +
                "data['operate_time'] operate_time,\n" +
                "data['order_status'] order_status,\n" +
                "`type`,\n" +
                "`old`,\n" +
                "ts oi_ts\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and (`type` = 'insert' or `type` = 'update')");
        tableEnv.createTemporaryView("order_info", orderInfo);

        // TODO 6.读取订单活动关联表数据 where table = 'order_detail_activity' and type = 'insert'
        Table orderDetailActivity = tableEnv.sqlQuery("select \n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['activity_id'] activity_id,\n" +
                "data['activity_rule_id'] activity_rule_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_activity'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // TODO 7.读取订单优惠券关联表数据 where table = 'order_detail_coupon' and type = 'insert'
        Table orderDetailCoupon = tableEnv.sqlQuery("select\n" +
                "data['order_detail_id'] order_detail_id,\n" +
                "data['coupon_id'] coupon_id\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_detail_coupon'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // TODO 8.从MySQL中读取字段表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 9.关联5张表数据 订单明细 join 订单 left join 订单活动 left join 订单优惠券 join base_dic
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "od.order_id,\n" +
                "od.sku_id,\n" +
                "od.sku_name,\n" +
                "date_format(od.create_time, 'yyyy-MM-dd') date_id,\n" +
                "od.create_time,\n" +
                "od.source_id,\n" +
                "od.source_type,\n" +
                "od.sku_num,\n" +
                "od.split_original_amount,\n" +
                "od.split_activity_amount,\n" +
                "od.split_coupon_amount,\n" +
                "od.split_total_amount,\n" +
                "od.od_ts,\n" +

                "oi.user_id,\n" +
                "oi.province_id,\n" +
                "date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id,\n" +
                "oi.operate_time,\n" +
                "oi.order_status,\n" +
                "oi.`type`,\n" +
                "oi.`old`,\n" +
                "oi.oi_ts,\n" +

                "act.activity_id,\n" +
                "act.activity_rule_id,\n" +

                "cou.coupon_id,\n" +

                "dic.dic_name source_type_name,\n" +

                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od \n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id = act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id = cou.order_detail_id\n" +
                "join `base_dic` for system_time as of od.proc_time as dic\n" +
                "on od.source_type = dic.dic_code");
        // current_row_timestamp()返回当前SQL带时区的系统时间，为了后续在DWS去重使用
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 10.构建订单域处理表，用来和Kafka做映射关系
        tableEnv.executeSql("" +
                "create table dwd_trade_order_pre_process(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "sku_name string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "source_id string,\n" +
                "source_type string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "split_total_amount string,\n" +
                "od_ts string,\n" +

                "user_id string,\n" +
                "province_id string,\n" +
                "operate_date_id string,\n" +
                "operate_time string,\n" +
                "order_status string,\n" +
                "`type` string,\n" +
                "`old` map<string,string>,\n" +
                "oi_ts string,\n" +

                "activity_id string,\n" +
                "activity_rule_id string,\n" +

                "coupon_id string,\n" +

                "source_type_name string,\n" +

                "row_op_ts timestamp_ltz(3),\n" +
                "primary key(id) not enforced\n" +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_pre_process"));

        // TODO 11.将数据写出到Kafka的dwd_trade_order_pre_process主题中
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_pre_process \n" +
                "select * from result_table");

    }
}
