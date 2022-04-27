package com.atguigu.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:32
 */
object RDD_SortBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd = sc.makeRDD(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
    // 默认正序排序
    rdd.sortByKey().collect().foreach(println)

    println("------------------------")

    // 配置为倒序排序
    rdd.sortByKey(false).collect().foreach(println)
    sc.stop()
  }
}
