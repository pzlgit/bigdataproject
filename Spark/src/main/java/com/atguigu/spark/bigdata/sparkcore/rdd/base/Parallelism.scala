package com.atguigu.spark.bigdata.sparkcore.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 并行度与分区
 *
 * @author pangzl
 * @create 2022-04-25 15:23
 */
object Parallelism {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sc.textFile("Data/word.txt", 2)
    rdd1.saveAsTextFile("Data/output1")
    rdd2.saveAsTextFile("Data/output2")
    sc.stop()
  }
}
