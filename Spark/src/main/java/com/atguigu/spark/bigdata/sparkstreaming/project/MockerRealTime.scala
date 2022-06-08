package com.atguigu.spark.bigdata.sparkstreaming.project

import com.atguigu.bigdata.sparkstreaming.project.RandomOptions.RanOpt
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @author pangzl
 * @create 2022-05-06 18:12
 */
object MockerRealTime {

  /**
   * 生产者生产消息发送到Kafka
   *
   * @param args 参数
   */
  def main(args: Array[String]): Unit = {
    // 1.获取配置文件config.properties中的Kafka配置文件信息
    val properties: Properties = PropertiesUtil.load("config.properties")
    val brokers: String = properties.getProperty("kafka.broker.list")
    val topic: String = properties.getProperty("kafka.topic")

    // 2.创建Kafka配置对象并添加配置信息
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 3.根据配置创建Kafka生产者
    val kafkaProducer = new KafkaProducer[String, String](prop)

    // 4.随机产生实时的数据并通过生产者发送到Kafka
    while (true) {
      for (data <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, data))
        println(data)
      }
      Thread.sleep(2000)
    }

  }

  /**
   * 模拟数据
   *
   * @return 模拟数据集合
   */
  def generateMockData(): Array[String] = {

    // 1.准备数据样本
    val array: ArrayBuffer[String] = ArrayBuffer[String]()
    val CityRandomOpt = RandomOptions(
      RanOpt(CityInfo(1, "北京", "华北"), 30),
      RanOpt(CityInfo(2, "上海", "华东"), 30),
      RanOpt(CityInfo(3, "广州", "华南"), 10),
      RanOpt(CityInfo(4, "深圳", "华南"), 20),
      RanOpt(CityInfo(5, "天津", "华北"), 10)
    )
    val random = new Random()

    // 2.准备模拟数据
    for (i <- 0 to 50) {
      val timestamp: Long = System.currentTimeMillis()

      val cityInfo: CityInfo = CityRandomOpt.getRandomOpt
      val city: String = cityInfo.city_name
      val area: String = cityInfo.area

      val adid: Int = random.nextInt(6) + 1
      val userid: Any = random.nextInt(6) + 1

      array += timestamp + " " + area + " " + city + " " + userid + " " + adid
    }

    array.toArray
  }

  // 自定义城市信息表
  case class CityInfo(city_id: Long, city_name: String, area: String)

}
