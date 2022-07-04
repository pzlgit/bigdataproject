package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Kafka 工具类
 *
 * @author pangzl
 * @create 2022-06-30 9:45
 */
public class MyKafkaUtil {

    private static final String KAFKA_HOST = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    // 获取消费者对象
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 创建FlinkKafkaConsumer对象
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        // TODO new SimpleStringSchema()的底层实现中，consumerRecord不能为空，否则会报错,因此需要自定义反序列化
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

    // 获取生产者对象
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");

        // todo：注意：通过FlinkKafkaProducer创建的对象默认的Semantic的值是AT_LEAST_ONCE，不能保证精准一次消费
        // todo: 需要使用如下的创建方式，指定Semantic为EXACTLY_ONCE才能保证精准一次消费
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "default_topic",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<>(topic, element.getBytes());
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        return kafkaProducer;
    }

}
