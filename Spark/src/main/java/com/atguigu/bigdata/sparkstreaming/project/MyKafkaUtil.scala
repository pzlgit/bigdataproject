package com.atguigu.bigdata.sparkstreaming.project

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * Kafka 工具类
 *
 * @author pangzl
 * @create 2022-05-06 18:47
 */
object MyKafkaUtil {

  // 获取配置文件信息
  private val properties: Properties = PropertiesUtil.load("config.properties")
  // 获取Kafka服务器配置信息
  private val brokers: String = properties.getProperty("kafka.broker.list")

  // 获取Kafka数据生成Dstream
  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    // 1.kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "commerce-consumer-group"
    )

    // 2.消费Kafka数据
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )

    // 3.返回流
    dstream
  }

}
