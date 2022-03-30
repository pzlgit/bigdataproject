package com.atguigu.bigdata.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 单消费者
 */
public class ConsumerSingle {

    public static void main(String[] args) {
        // Kafka配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 定义Kafka消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        // 创建消费者客户端
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // 注册消费的主题名称
        kafkaConsumer.subscribe(Collections.singletonList("second"));
        // 调用客户端消费消息
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(2L));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

}