package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Filter
 *
 * @author pangzl
 * @create 2022-04-26 20:10
 */
object RDD_Filter {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // filter 创建一个RDD，过滤出对2取余等于0（偶数）的数据。
    val rdd1 = sc.makeRDD(1 to 4, 2)
    val rdd2 = rdd1.filter(_ % 2 == 0)
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
