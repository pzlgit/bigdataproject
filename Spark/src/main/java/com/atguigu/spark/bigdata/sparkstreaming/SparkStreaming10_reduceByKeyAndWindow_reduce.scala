package com.atguigu.spark.bigdata.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
 *
 * @author pangzl
 * @create 2022-05-05 11:00
 */
object SparkStreaming10_reduceByKeyAndWindow_reduce {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("./ck")

    // 3.统计WordCount：3秒一个批次，窗口12秒，滑步6秒。使用反向Reduce
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDs: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1))
    wordDs.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      (a: Int, b: Int) => a - b,
      Seconds(12),
      Seconds(6),
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0
    ).print()

//    wordDs.countByWindow(Seconds(12), Seconds(6)).print()
//    val func = (x: (String, Int), y: (String, Int)) => {
//      (x._1, x._2 + y._2)
//    }
//    wordDs.reduceByWindow(func, Seconds(12), Seconds(6)).print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
