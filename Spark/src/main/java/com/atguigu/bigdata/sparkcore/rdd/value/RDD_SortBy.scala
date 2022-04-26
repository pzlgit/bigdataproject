package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * SortBy
 *
 * @author pangzl
 * @create 2022-04-26 20:50
 */
object RDD_SortBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // sortBy排序
    // 创建一个RDD，按照数字大小分别实现正序和倒序排序。
    val rdd1 = sc.makeRDD(List(1, 3, 2, 4))
    // 默认是升序排序
    val rdd2 = rdd1.sortBy(num => num)
    rdd2.collect().foreach(println(_))
    println("____________________")
    // 配置为降序排序
    val rdd3 = rdd1.sortBy(num => num, false)
    rdd3.collect().foreach(println(_))
    println("____________________")
    // 创建一个RDD
    val rdd4 = sc.makeRDD(Array((2, 1), (1, 2), (1, 1), (2, 2)))
    rdd4.sortBy(t => t).collect().foreach(println(_))
    sc.stop()
  }
}
