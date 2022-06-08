package com.atguigu.spark.bigdata.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 21:04
 */
object RDD_Foreach {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // foreach
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // 单点集合顺序打印，按照分区采集数据
    rdd.collect().foreach(println)
    println("********************")
    // 分布式打印，乱序打印
    rdd.foreach(println(_))
    sc.stop()
  }
}
