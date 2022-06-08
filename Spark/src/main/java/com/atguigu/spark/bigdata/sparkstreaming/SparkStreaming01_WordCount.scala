package com.atguigu.spark.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * WordCount 案例
 *
 * @author pangzl
 * @create 2022-05-05 9:18
 */
object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {
    // 1. 初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2. 初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 3. wordCount 实现逻辑
    val lineDstream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    lineDstream.flatMap(_.split(" "))
      .map(((_, 1)))
      .reduceByKey(_ + _)
      .print()

    // 4. 启动SparkStreaming
    ssc.start()
    // 5. 阻塞主线程
    ssc.awaitTermination()
  }

}
