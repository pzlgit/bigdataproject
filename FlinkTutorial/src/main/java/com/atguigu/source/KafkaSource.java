package com.atguigu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * 从Kafka读取数据
 *
 * @author pangzl
 * @create 2022-06-18 19:31
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Kafka消费者配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        // 从Kafka中读取数据，flink并没有提供预实现方法，提供了一个连接工具，flink-connector-kafka,实现了一个flinkKafkaConsumer
        env.addSource(new FlinkKafkaConsumer(
                        Arrays.asList("clicks", "clicks1"),
                        new SimpleStringSchema(),
                        properties
                ))
                .print("FlinkKafkaConsumer");

        env.execute();
    }
}
