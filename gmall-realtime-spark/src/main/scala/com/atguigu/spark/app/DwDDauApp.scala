package com.atguigu.spark.app

import com.alibaba.fastjson.JSON
import com.atguigu.spark.bean.PageLog
import com.atguigu.spark.utils.{MyKafkaUtils, MyOffsetUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日活宽表处理
 *
 * @author pangzl
 * @create 2022-06-10 11:08
 */
object DwDDauApp {

  def main(args: Array[String]): Unit = {
    // 1.创建实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2.从Redis中读取Kafka偏移量
    val topic = "DWD_PAGE_LOG"
    val groupId = "dwd_dau_group"
    val offsetMap: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(topic, groupId)
    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.nonEmpty) {
      // 指定offset位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, offsetMap, groupId)
    } else {
      // 默认offset位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, groupId)
    }

    // 3.读取Kafka偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    kafkaDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 4.转换数据结构
    val pageLogDStream: DStream[PageLog] = kafkaDStream.map(
      record => {
        val message: String = record.value()
        val pageLog: PageLog = JSON.parseObject(message, classOf[PageLog])
        pageLog
      }
    )

    pageLogDStream.print(10)

    ssc.start()
    ssc.awaitTermination()
  }

}
