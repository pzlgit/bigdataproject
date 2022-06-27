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
 * @create 2022-06-26 15:40
 */
public class TableExample {

    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据源
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
        // 将数据流转化为动态表
        Table table = tableEnv.fromDataStream(eventStream);
        // 用执行SQL的方式提取数据
        Table visitTable = tableEnv.sqlQuery("select url,user from " + table);
        // 将动态表转换为数据流打印输出
        tableEnv.toDataStream(visitTable).print();

        // 使用Table API
        Table select = table.select($("url"), $("user"));
        tableEnv.toDataStream(select).print();

        env.execute();
    }
}
