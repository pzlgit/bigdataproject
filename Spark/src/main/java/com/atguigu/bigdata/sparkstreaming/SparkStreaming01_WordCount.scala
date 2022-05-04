package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author pangzl
 * @create 2022-05-04 18:42
 */
object SparkStreaming01_WordCount {

  def main(args: Array[String]): Unit = {
    // 构建SparkStreaming环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // netcat 发送数据，接收数据并进行wordCount
    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDstream: DStream[String] = socketDstream.flatMap(_.split(" "))
    val tupleDstream: DStream[(String, Int)] = wordDstream.map((_, 1))
    val countResult: DStream[(String, Int)] = tupleDstream.reduceByKey(_ + _)

    countResult.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
