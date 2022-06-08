package com.atguigu.spark.bigdata.sparkcore.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 创建
 *
 * @author pangzl
 * @create 2022-04-25 15:10
 */
object CreateRDD {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
    val sc = new SparkContext(sparkConf)
    // makeRDD 创建 RDD
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    rdd1.collect().foreach(println)
    println("----------------------------")
    // parallelize 创建 RDD
    val rdd2: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    rdd2.collect().foreach(println(_))

    println("----------------------------")
    // 从外部储存文件创建RDD
    // 1、从本地文件系统读取文件创建RDD
    val rdd3: RDD[String] = sc.textFile("Data")
    rdd3.foreach(println(_))
    println("***********")
    val rdd4: RDD[String] = sc.textFile("Data/Hello.txt,Data/word.txt")
    rdd4.foreach(println(_))
    // 2、从HDFS读取文件创建RDD
    val rdd5: RDD[String] = sc.textFile("hdfs://hadoop102:9820/wcinput")
    rdd5.foreach(println(_))
    sc.stop()
  }
}
