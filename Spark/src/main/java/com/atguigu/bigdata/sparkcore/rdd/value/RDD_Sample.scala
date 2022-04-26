package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Sample 采样
 *
 * @author pangzl
 * @create 2022-04-26 20:19
 */
object RDD_Sample {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD（1-10），从中选择放回和不放回抽样。
    val rdd1 = sc.makeRDD(1 to 6)
    // 抽取数据不放回
    val sampleRDD: RDD[Int] = rdd1.sample(false, 1)
    sampleRDD.collect().foreach(println)

    println("-------------------------------")
    // 抽取数据放回
    val sampleRDD1: RDD[Int] = rdd1.sample(true, 2)
    sampleRDD1.collect().foreach(println)
    sc.stop()
  }
}
