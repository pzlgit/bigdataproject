package com.atguigu.sql;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 综合应用示例
 * 用户的一组点击事件，我们可以查询出某个用户（例如Alice）点击的url列表，
 * 也可以统计出每个用户累计的点击次数，这可以用两句SQL来分别实现。
 *
 * @author pangzl
 * @create 2022-06-30 14:55
 */
public class TableToStreamExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转化为表
        tableEnv.createTemporaryView("EventTable", eventStream);

        // 查询Alice的访问列表
        Table aliceTable = tableEnv.sqlQuery("select url,user from EventTable where user = 'Alice'");

        // 统计每个用户的点击次数
        Table countTable = tableEnv.sqlQuery("select user,count(url) from EventTable group by user");

        // 将表转化为数据流打印输出
        tableEnv.toDataStream(aliceTable).print("url");
        tableEnv.toChangelogStream(countTable).print("count");

        env.execute();
    }
}
