package com.atguigu.spark.bigdata.sparkstreaming

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.net.URI

/**
 *
 * @author pangzl
 * @create 2022-05-05 11:30
 */
object SparkStreaming12_stop {

  def main(args: Array[String]): Unit = {
    // 1.初始化Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    // 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3. 设置优雅关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    ds.flatMap(_.split(" ")).map((_, 1)).print()

    // 开启一个线程开启监控程序
    new Thread(new MonitorStop(ssc)).start()

    // 4.启动任务并阻塞主线程
    ssc.start()
    ssc.awaitTermination()
  }

  class MonitorStop(ssc: StreamingContext) extends Runnable {
    override def run(): Unit = {
      // 获取HDFS文件系统
      val system: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9820"), new Configuration(), "atguigu")
      while (true) {
        Thread.sleep(5000)
        val exists = system.exists(new Path("hdfs://hadoop102:9820/stopSpark"))
        if (exists) {
          val state: StreamingContextState = ssc.getState()
          if (StreamingContextState.ACTIVE == state) {
            ssc.stop(stopSparkContext = true, stopGracefully = true)
            System.exit(0)
          }
        }
      }
    }
  }
}
