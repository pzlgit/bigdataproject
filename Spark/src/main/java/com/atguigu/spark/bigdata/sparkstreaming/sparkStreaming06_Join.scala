package com.atguigu.spark.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * join
 *
 * @author pangzl
 * @create 2022-05-05 10:17
 */
object sparkStreaming06_Join {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 3.两个流之间join
    val ds1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val ds2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    val word1: DStream[(String, Int)] = ds1.flatMap(_.split(" ")).map((_, 1))
    val word2: DStream[(String, String)] = ds2.flatMap(_.split(" ")).map((_, "a"))

    val result: DStream[(String, (Int, String))] = word1.join(word2)
    result.print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
