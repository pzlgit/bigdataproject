package com.atguigu.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author pangzl
 * @create 2022-05-04 20:02
 */
object JoinTest {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(6))
    // 3. join 两个流
    // 3.1 从Socket创建两个流
    val ds1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 4444)
    val ds2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 5555)
    // 3.2 将两个流转化为kv类型
    val wordDs1: DStream[(String, Int)] = ds1.flatMap(_.split(" ")).map((_, 1))
    val wordDs2: DStream[(String, String)] = ds2.flatMap(_.split(" ")).map((_, "a"))

    // 3.3 流的join
    val joinDs: DStream[(String, (Int, String))] = wordDs1.join(wordDs2)

    joinDs.print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }
}
