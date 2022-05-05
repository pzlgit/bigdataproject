package com.atguigu.bigdata.sparkstreaming

import jline.internal.InputStreamReader
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.BufferedReader
import java.net.Socket
import java.nio.charset.StandardCharsets

/**
 * 需求：自定义数据源，实现监控某个端口号，获取该端口号内容。
 *
 * @author pangzl
 * @create 2022-05-05 9:36
 */
object SparkStreaming03_CustomerReceiver {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3. 自定义数据源采集数据
    val lineDs: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("localhost", 9999))
    lineDs.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 自定义采集器
   * String ： 返回值类型
   * StorageLevel.MEMORY_ONLY ： 返回值存储方式 内存
   */
  class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    // 采集器启动时调用，作用:读取数据并将数据发送给Spark
    override def onStart(): Unit = {
      // 开启一个线程专门用于采集数据
      new Thread("Socket Receiver") {
        override def run() = {
          receiver()
        }
      }.start()
    }

    // 采集逻辑
    def receiver(): Unit = {
      // 1. 创建一个Socket
      val socket = new Socket(host, port)
      // 2. 创建BufferReader用于读取端口传来的数据
      val reader = new BufferedReader(new InputStreamReader(
        socket.getInputStream, StandardCharsets.UTF_8
      ))
      // 3.读取数据
      var line: String = reader.readLine()
      // 4. 当Receiver没有关闭且输入数据不为空，则循环发送数据给Spark
      while (!isStopped() && line != null) {
        store(line)
        line = reader.readLine()
      }

      // 5. 如果循环结束，则关闭资源
      reader.close()
      socket.close()

      // 6.重启接收任务
      restart("restart")
    }

    // 采集器关闭的时候调用
    override def onStop(): Unit = {

    }

  }
}
