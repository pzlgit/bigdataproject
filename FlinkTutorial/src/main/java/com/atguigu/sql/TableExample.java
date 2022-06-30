package com.atguigu.sql;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Table API 和 SQL 简单示例
 *
 * @author pangzl
 * @create 2022-06-30 14:28
 */
public class TableExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从元素中读取数据
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );
        // 获取表执行环境
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        // 将数据流转换成动态表
        Table table = tableEnv.fromDataStream(eventStream);
        // 执行SQL提取数据，返回的还是动态表
        //Table selectTable = tableEnv.sqlQuery("select user , url from " + table + " where user = 'Alice'");
        tableEnv.createTemporaryView("EventTable", table);
        Table selectTable = tableEnv.sqlQuery("select user,url from EventTable");
        // 将动态表数据转化为数据流打印输出
        tableEnv.toDataStream(selectTable).print("SQL");

        // 使用 Table API 方式读取数据
        Table selectByTable = table.select($("user"), $("url"));
        tableEnv.toDataStream(selectByTable).print("Table API");
        env.execute();
    }
}
