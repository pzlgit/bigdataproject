package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 通过Transform将DStream每一批次的数据直接转换为RDD的算子操作
 *
 * @author pangzl
 * @create 2022-05-05 10:10
 */
object SparkStreaming05_Transform {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3. 通过Transform将DStream每一批次的数据直接转换为RDD的算子操作
    val lineDsteam: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 在Driver端执行，全局一次
    println("111111111:" + Thread.currentThread().getName)

    val ds: DStream[(String, Int)] = lineDsteam.transform(
      rdd => {
        // 在Driver端执行(ctrl+n JobGenerator)，一个批次一次
        println("222222:" + Thread.currentThread().getName)

        val wordRDD = rdd.flatMap(_.split(" "))
        val result: RDD[(String, Int)] = wordRDD.map(
          d => {
            // 在Executor端执行，执行次数和单词个数相同
            println("333333:" + Thread.currentThread().getName)
            (d, 1)
          }
        )
        result.reduceByKey(_ + _)
      }
    )

    ds.print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
