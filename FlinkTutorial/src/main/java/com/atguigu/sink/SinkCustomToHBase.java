package com.atguigu.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.nio.charset.StandardCharsets;

/**
 * 将数据写入到HBase
 *
 * @author pangzl
 * @create 2022-06-19 11:57
 */
public class SinkCustomToHBase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.fromElements("hello", "world")
                .addSink(
                        new RichSinkFunction<String>() {

                            // 管理HBase配置信息
                            public org.apache.hadoop.conf.Configuration configuration;

                            // 管理HBase连接
                            public Connection connection;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                // 创建HBase连接
                                configuration = HBaseConfiguration.create();
                                configuration.set("hbase.zookeeper.quorum", "hadoop102:2181");
                                connection = ConnectionFactory.createConnection(configuration);
                            }

                            @Override
                            public void close() throws Exception {
                                super.close();
                                connection.close();
                            }

                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                Table table = connection.getTable(TableName.valueOf("flink"));
                                Put put = new Put("rowKey".getBytes(StandardCharsets.UTF_8));
                                put.addColumn(
                                        "info".getBytes(StandardCharsets.UTF_8),
                                        value.getBytes(StandardCharsets.UTF_8),
                                        "1".getBytes(StandardCharsets.UTF_8)
                                );
                                table.put(put);
                                table.close();
                            }
                        }
                );
        env.execute();
    }
}
