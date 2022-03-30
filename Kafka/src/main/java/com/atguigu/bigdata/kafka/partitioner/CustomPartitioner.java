package com.atguigu.bigdata.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 */
public class CustomPartitioner implements Partitioner {

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String data = value.toString();
        if (data.contains("atguigu")) {
            return 0;
        }
        return 1;
    }

    /**
     * 关闭资源
     */
    public void close() {
    }

    /**
     * Config 配置信息
     *
     * @param configs
     */
    public void configure(Map<String, ?> configs) {
        System.out.println(configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        System.out.println(configs.get(ProducerConfig.ACKS_CONFIG));
        System.out.println(configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

}