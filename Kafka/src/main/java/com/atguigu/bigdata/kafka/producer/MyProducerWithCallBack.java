package com.atguigu.bigdata.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 带回调函数的异步发送
 */
public class MyProducerWithCallBack {

    public static void main(String[] args) {
        // Kafka 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 创建Kafka对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 500; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("second", i + "", "atguigu" + i), new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                System.out.println("Topic:" + metadata.topic() + "--" + "Partition:" + metadata.partition());
                            } else {
                                System.out.println(exception.getMessage());
                            }
                        }
                    }
            );
        }
        // 关闭Kafka资源
        kafkaProducer.close();
    }

}