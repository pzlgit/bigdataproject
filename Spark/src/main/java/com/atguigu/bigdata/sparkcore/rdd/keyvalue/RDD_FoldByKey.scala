package com.atguigu.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey
 *
 * @author pangzl
 * @create 2022-04-27 19:08
 */
object RDD_FoldByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 分区内和分区键相同的aggregateByKey
    val list = List(("a", 1), ("a", 1), ("a", 1), ("b", 1))
    val rdd1 = sc.makeRDD(list, 2)
    val rdd3 = rdd1.foldByKey(0)(_ + _)
    rdd3.collect().foreach(println)
    sc.stop()
  }
}
