package com.atguigu.bigdata.sparkcore.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-25 16:02
 */
object partition04_file {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile("Data/3.txt", 3).saveAsTextFile("Data/output5")
    sc.stop()
  }
}
