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
 *
 * @author pangzl
 * @create 2022-05-04 19:02
 */
object SparkStreaming03_CustomerReceiver {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3. 自定义数据源
    val ds: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("localhost", 9999))
    ds.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }

  // 自定义数据源
  class CustomerReceiver(host: String, port: Int) extends
    Receiver[String](StorageLevel.MEMORY_ONLY) {

    // 初始化启动的时候执行逻辑
    override def onStart(): Unit = {
      // 开启一个线程读取数据
      new Thread("Socket Receiver") {
        override def run() = {
          // 调用方法
          receiver()
        }
      }.start()
    }

    def receiver(): Unit = {
      // 创建一个Socket
      val socket = new Socket(host, port)
      // 创建一个BufferedReader用于读取端口传递来的数据
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
      // 读取数据
      var data: String = reader.readLine()
      // 当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
      while (!isStopped() && data != null) {
        // 存储数据
        store(data)
        data = reader.readLine()
      }
      // 如果循环结束，则关闭资源
      reader.close()
      socket.close()

      // 重启接收任务
      restart("restart")
    }

    // 当采集器停止时执行逻辑
    override def onStop(): Unit = {
    }

  }

}

