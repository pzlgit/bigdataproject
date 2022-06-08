package com.atguigu.spark.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties
import scala.collection.mutable

/**
 * Kafka工具类，用于生产或者消费数据
 *
 * @author pangzl
 * @create 2022-06-08 18:46
 */
object MyKafkaUtils {

  /**
   * Kafka消费者配置
   */
  private val consumerConfig = mutable.Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils("kafka.bootstrap.servers"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"
  )

  /**
   * 消费数据（默认offsets消费数据）
   */
  def getKafkaDStream(
                       topic: String,
                       ssc: StreamingContext,
                       groupId: String
                     ): InputDStream[ConsumerRecord[String, String]] = {
    // 消费者配置中增加消费者组名称
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    // SparkStreaming读取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Array(topic),
          consumerConfig
        )
      )
    kafkaDStream
  }

  /**
   * 消费数据（指定offsets消费数据）
   */
  def getKafkaDStream(topic: String,
                      ssc: StreamingContext,
                      offsets: Map[TopicPartition, Long],
                      groupId: String
                     ): InputDStream[ConsumerRecord[String, String]] = {
    // 消费者配置中增加消费者组名称
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    // SparkStreaming读取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Array(topic),
          consumerConfig,
          offsets
        )
      )
    kafkaDStream
  }

  private var producer: KafkaProducer[String, String] = createKafkaProducer()

  /**
   * 创建Kafka客户端
   */
  def createKafkaProducer(): KafkaProducer[String, String] = {
    // 生产者配置
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropertiesUtils("kafka.bootstrap.servers"))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // 创建Kafka生产者
    val producer = new KafkaProducer[String, String](properties)
    producer
  }

  /**
   * Kafka发送数据
   */
  def send(topic: String, message: String) = {
    producer.send(new ProducerRecord[String, String](topic, message))
  }

  /**
   * Kafka指定Key发送数据
   */
  def send(topic: String, key: String, message: String) = {
    producer.send(new ProducerRecord[String, String](topic, key, message))
  }

  /**
   * 将Kafka内存中的数据刷写到Broker
   */
  def flush(): Unit = {
    if (producer != null) {
      producer.flush()
    }
  }

  /**
   * 关闭生产者对象
   */
  def close(): Unit = {
    if (producer != null) {
      producer.close()
    }
  }

}
