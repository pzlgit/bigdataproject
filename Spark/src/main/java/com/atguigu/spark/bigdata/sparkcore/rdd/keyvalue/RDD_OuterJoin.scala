package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:48
 */
object RDD_OuterJoin {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // leftOuterJoin rightOuterJoin FullOuterJoin
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2 = sc.makeRDD(List(("a", 1), ("d", 2), ("c", 3)))
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    println("*********************")
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)
    println("*********************")
    rdd1.fullOuterJoin(rdd2).collect().foreach(println)
    sc.stop()
  }
}
