package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * Kafka 工具类
 *
 * @author pangzl
 * @create 2022-06-30 9:45
 */
public class MyKafkaUtil {

    private static final String KAFKA_HOST = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    // 从Kafka读取数据，创建getKafkaConsumer(String topic, String groupId)方法
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        // new SimpleStringSchema()的底层实现中，consumerRecord不能为空，否则会报错,因此需要自定义反序列化
                        if (consumerRecord != null && consumerRecord.value() != null) {
                            return new String(consumerRecord.value());
                        }
                        return null;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                },
                properties
        );
        return kafkaConsumer;
    }

}
