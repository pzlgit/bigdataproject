package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author pangzl
 * @create 2022-05-05 10:53
 */
object SparkStreaming09_reduceByKeyAndWindow {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // 3.统计WordCount：3秒一个批次，窗口12秒，滑步6秒。
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDs: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1))
    val windowDs: DStream[(String, Int)] = wordDs.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(12), Seconds(6))
    windowDs.print()
    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }

}
