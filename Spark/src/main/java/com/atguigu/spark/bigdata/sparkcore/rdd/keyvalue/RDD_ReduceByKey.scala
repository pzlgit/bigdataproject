package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * ReduceByKey
 *
 * @author pangzl
 * @create 2022-04-27 18:50
 */
object RDD_ReduceByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // reduceByKey 根据K对V进行预聚合
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("a", 5), ("b", 2), ("c", 1)
    ))
    //    val rdd2 = rdd1.reduceByKey(
    //      (x, y) => {
    //        x + y
    //      }
    //    )
    val rdd2 = rdd1.reduceByKey(_ + _)
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
