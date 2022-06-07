package com.atguigu.spark.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties
import scala.collection.mutable

/**
 * Kafka操作工具类，用于生产消息和消费消息
 *
 * @author pangzl
 * @create 2022-06-07 18:26
 */
object MyKafkaUtils {

  // Kafka消费者配置信息
  private val consumerConfig: mutable.Map[String, String] = mutable.Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtils("kafka.bootstrap.servers"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // Offsets重置 latest(默认)：重置到结束的位置，已有的数据不会消费
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    // 自动提交Offsets,默认开启
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 自动提交Offsets的时间频率，默认5000ms
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"
  )

  /**
   * 消费者消费消息（默认offsets消费数据）
   */
  def getKafkaDStream( topicName: String,
                       scc: StreamingContext,
                      groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    // 增加消费者组配置
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    // 1.使用Kafka提供的工具类消费消息
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        scc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Array(topicName),
          consumerConfig
        )
      )
    kafkaDStream
  }

  // 创建生产者对象
  var producer: KafkaProducer[String, String] = createKafkaProducer()

  /**
   * 构建生产者对象
   */
  def createKafkaProducer(): KafkaProducer[String, String] = {
    // Kafka生产者配置
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      PropertiesUtils("kafka.bootstrap.servers"))
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    // 生产者端开启幂等性
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    // 创建生产者
    val producer = new KafkaProducer[String, String](properties)
    producer
  }

  /**
   * 生产者生产消息
   */
  def send(topicName: String, value: String): Unit = {
    producer.send(new ProducerRecord[String, String](topicName, value))
  }


}
