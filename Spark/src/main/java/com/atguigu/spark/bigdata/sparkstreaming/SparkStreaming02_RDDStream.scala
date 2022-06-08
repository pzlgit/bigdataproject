package com.atguigu.spark.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 循环创建几个RDD，将RDD放入队列。通过SparkStreaming创建Dstream，计算WordCount。
 *
 * @author pangzl
 * @create 2022-05-05 9:26
 */
object SparkStreaming02_RDDStream {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    // 3. Queue 创建 Dstream
    val queue = new mutable.Queue[RDD[Int]]()

    val queueDstream: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = false)

    queueDstream.reduce(_ + _).print()

    // 4.启动任务并阻塞主线程
    ssc.start()

    // 循环向Queue中增加数据
    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
