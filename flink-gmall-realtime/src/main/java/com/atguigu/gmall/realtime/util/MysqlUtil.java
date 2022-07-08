package com.atguigu.gmall.realtime.util;

/**
 * 将MySQL数据库中的字段表读取为Lookup表，以便进行维度退化
 *
 * @author pangzl
 * @create 2022-07-08 18:59
 */
public class MysqlUtil {

    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`(" +
                "`dic_code` string," +
                "`dic_name` string," +
                "`parent_code` string," +
                "`create_time` timestamp," +
                "`operate_time` timestamp," +
                "primary key(`dic_code`) not enforced" +
                ")" + mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        String ddl = "WITH (" +
                "'connector' = 'jdbc'," +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall'," +
                "'table-name' = '" + tableName + "'," +
                "'lookup.cache.max-rows' = '500'," +
                "'lookup.cache.ttl' = '1 hour'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'" +
                ")";
        return ddl;
    }

}
