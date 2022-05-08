package com.atguigu.bigdata.sparkcore.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量
 *
 * @author pangzl
 * @create 2022-05-08 15:22
 */
object broadcast01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("WARN:Class Not Find", "INFO:Class Not Find", "DEBUG:Class Not Find"), 4)
    val list: String = "WARN"

    val warn: Broadcast[String] = sc.broadcast(list)

    rdd.filter(
      word =>{
        word.contains(warn.value)
      }
    ).foreach(println(_))

    sc.stop()
  }
}
