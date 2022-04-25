package com.atguigu.bigdata.sparkcore.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Map 映射
 *
 * @author pangzl
 * @create 2022-04-25 18:44
 */
object RDD_Map {

  def main(args: Array[String]): Unit = {
    // map映射 - 元素乘以2
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建RDD
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4, 2)
    // 使用Map将数据映射为元素乘以2
    // val rdd2 = rdd1.map(_ * 2)
    val rdd2 = rdd1.map(
      number => {
        println(number)
        number * 2
      }
    )
    // rdd2.collect().foreach(println)
    // map操作后分区中的元素不变，只是改变内容
    rdd2.saveAsTextFile("Data/output")
    sc.stop()
  }
}
