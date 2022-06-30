package com.atguigu.sql;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 追加查询案例
 *
 * @author pangzl
 * @create 2022-06-30 15:15
 */
public class AppendQueryExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据源数据，并指定时间戳和水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                );

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将流数据转化成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts") // 将timestamp指定为事件事件，并重命名为ts
        );
        // 注册表
        tableEnv.createTemporaryView("EventTable", eventTable);
        // 设置1小时滚动窗口，执行SQL统计查询
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "user, " +
                        "window_end AS endT, " +    // 窗口结束时间
                        "COUNT(url) AS cnt " +    // 统计url访问次数
                        "FROM TABLE( " +
                        "TUMBLE( TABLE EventTable, " +    // 1小时滚动窗口
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '1' HOUR)) " +
                        "GROUP BY user, window_start, window_end "
        );
        tableEnv.toDataStream(result).print();
        env.execute();
    }
}
