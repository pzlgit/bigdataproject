package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author pangzl
 * @create 2022-05-04 19:52
 */
object SparkStreaming05_Transform {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // 3. Transform 将DStream 转化为 RDD
    val socketDs: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    // 在Driver端执行，全局执行一次
    println("111111:" + Thread.currentThread().getName)
    val result: DStream[(String, Int)] = socketDs.transform(
      rdd => {
        // 在Driver端执行(ctrl+n JobGenerator)，一个批次一次
        println("222222:" + Thread.currentThread().getName)

        val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(
          data => {
            // 在Executor端执行，执行次数和单词个数相同
            println("333333:" + Thread.currentThread().getName)
            (data, 1)
          })
        val result: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)
        result
      }
    )

    // 打印
    result.print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
