package com.atguigu.gmall.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 演示：Flink jdbc connector 以及 LookUpJoin
 *
 * @author pangzl
 * @create 2022-07-07 19:23
 */
public class Test03_JdbcConnector {

    public static void main(String[] args) {
        // TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.从Kafka中读取数据并创建动态表
        tableEnv.executeSql(" CREATE TABLE emp (\n" +
                "                `empno` integer,\n" +
                "                `ename` String,\n" +
                "                `deptno` integer,\n" +
                "               proc_time as proctime()\n" +
                "        ) WITH (\n" +
                "                'connector' = 'kafka',\n" +
                "                'topic' = 'first',\n" +
                "                'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "                'properties.group.id' = 'testGroup',\n" +
                "                'scan.startup.mode' = 'latest-offset',\n" +
                "                'format' = 'json'\n" +
                "        )");

        // TODO 3.使用Jdbc连接器创建动态表映射MySQL数据库中的表
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                "  deptno integer,\n" +
                "  dname STRING,\n" +
                "  ts BIGINT\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall_config',\n" +
                "   'table-name' = 't_dept',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "'lookup.cache.max-rows' = '500'," +
                "'lookup.cache.ttl' = '1 hour'," +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")\n");

        // TODO 4.Lookup join 测试
        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e left join dept FOR SYSTEM_TIME AS OF proc_time d on e.deptno = d.deptno").print();
    }
}
