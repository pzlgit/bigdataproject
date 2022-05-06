package com.atguigu.bigdata.sparkstreaming.project

import com.atguigu.bigdata.sparkstreaming.project.BlackListHandler.Ads_log
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

/**
 * 广告黑名单实现
 *
 * @author pangzl
 * @create 2022-05-06 19:27
 */
object RealtimeApp {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // 3.广告黑名单实现
    // 3.1 读取数据获取Kafka配置信息
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(topic, ssc)

    // 3.2 将从Kafka读出的数据转化为样例类对象
    val adsLogDstream: DStream[Ads_log] = kafkaDstream.map(
      message => {
        val value: String = message.value()
        val arr: Array[String] = value.split(" ")
        Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
      }
    )

    // 需求一：根据MySQL中的黑名单过滤当前数据集
    val filterAdsLogDstream: DStream[Ads_log] =
      BlackListHandler.filterByBlackList(adsLogDstream)
    // 需求一：将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filterAdsLogDstream)

    // 测试打印
    filterAdsLogDstream.cache()
    filterAdsLogDstream.count().print()

    println("==============================")

    // 需求二：统计每天各大区各个城市广告点击总数并保存至MySQL中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDstream)

    // 需求三：统计最近一个小时（2分钟）广告分时，点击总数
    val result3: DStream[(String, List[(String, Long)])] = LastHourAdCountHandler.getAdHourMintToCount(filterAdsLogDstream)
    result3.print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }

}
