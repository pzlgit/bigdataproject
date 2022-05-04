package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 *
 * @author pangzl
 * @create 2022-05-04 18:51
 */
object SparkStreaming02_RDDStream {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    // 3. 逻辑处理 - RDD 创建 Dstream
    val queue = new mutable.Queue[RDD[Int]]()
    val ds: InputDStream[Int] = ssc.queueStream(queue, oneAtATime = true)
    val countDs: DStream[Int] = ds.reduce(_ + _)
    countDs.print()

    // 4.启动任务并阻塞主线程
    ssc.start()

    // 循环创建并向queue队列中放入RDD
    for (i <- 1 to 5) {
      val rdd: RDD[Int] = ssc.sparkContext.makeRDD(1 to 5)
      queue += rdd
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }

}
