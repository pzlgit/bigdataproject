package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * FlatMap
 *
 * @author pangzl
 * @create 2022-04-26 19:40
 */
object RDD_FlatMap {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个集合，集合里面存储的还是子集合，把所有子集合中数据取出放入到一个大的集合中。
    val rdd1 = sc.makeRDD(List(
      List(1, 2), List(3, 4), List(5, 6), List(7, 8)
    ))
    val rdd2 = rdd1.flatMap(list => list)
    rdd2.collect().foreach(println(_))
    sc.stop()
  }
}
