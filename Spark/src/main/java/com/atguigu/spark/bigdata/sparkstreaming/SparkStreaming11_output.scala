package com.atguigu.spark.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author pangzl
 * @create 2022-05-05 11:24
 */
object SparkStreaming11_output {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3.输出
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordDs: DStream[(String, Int)] = ds.flatMap(_.split(" ")).map((_, 1))

    // 在Driver端执行一次
    println("111111:" + Thread.currentThread().getName)

    wordDs.foreachRDD(
      rdd => {
        // 在Driver端执行(ctrl+n JobScheduler)，一个批次一次
        // 在JobScheduler 中查找（ctrl + f）streaming-job-executor
        println("222222:" + Thread.currentThread().getName)
        rdd.foreachPartition(
          iter => {
            iter.foreach(println)
          }
        )

      }
    )

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
