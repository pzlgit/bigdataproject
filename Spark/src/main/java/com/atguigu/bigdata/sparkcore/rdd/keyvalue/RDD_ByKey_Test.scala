package com.atguigu.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:24
 */
object RDD_ByKey_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // reduceByKey、foldByKey、aggregateByKey、combineByKey的区别
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ))
    // 求WordCount
    rdd.reduceByKey(_ + _)
    rdd.aggregateByKey(0)(_ + _, (_ + _))
    rdd.foldByKey(0)(_ + _)
    rdd.combineByKey(v => v, (t: Int, v) => t + v, (t1: Int, t2: Int) => t1 + t2)
    sc.stop()
  }
}
