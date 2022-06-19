package com.atguigu.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * 将数据写道Kafka
 *
 * @author pangzl
 * @create 2022-06-19 11:00
 */
public class SinkToKafkaTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.readTextFile("input/words.txt")
                .addSink(new FlinkKafkaProducer<String>(
                        "hadoop102:9092",
                        "clicks",
                        new SimpleStringSchema()
                ));
        env.execute();
    }
}
