package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author pangzl
 * @create 2022-05-04 20:32
 */
object SparkStreaming08_reduceByKeyAndWindow {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val wordDs: DStream[(String, Int)] = ssc.socketTextStream("localhost", 9998)
      .flatMap(_.split(" "))
      .map((_, 1))
    val result: DStream[(String, Int)] = wordDs.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y, Seconds(12), Seconds(6))
    result.print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
