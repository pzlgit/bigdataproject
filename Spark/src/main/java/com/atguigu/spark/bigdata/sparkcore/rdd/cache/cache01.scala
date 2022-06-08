package com.atguigu.spark.bigdata.sparkcore.rdd.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Cache 缓存
 *
 * @author pangzl
 * @create 2022-05-08 11:21
 */
object cache01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD,指定读取文件的位置
    val fileRDD: RDD[String] = sc.textFile("data/word.txt")
    // wordCount
    val rdd1: RDD[String] = fileRDD.flatMap(_.split(" "))
    val rdd2: RDD[(String, Int)] = rdd1.map(
      word => {
        println("***")
        (word, 1)
      }
    )

    // 数据缓存
//    rdd2.cache()
    // 更改存储级别
    rdd2.persist(StorageLevel.MEMORY_ONLY)

    println(rdd2.toDebugString)

    // 触发执行逻辑
    rdd2.collect()
    println("-----------------")
    println(rdd2.toDebugString)

    // 3.6 再次触发执行逻辑（有缓存就不会输出*）
    rdd2.collect()

    sc.stop()
  }

}
