package com.atguigu.bigdata.sparkcore.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-25 15:58
 */
object partition03_file_default {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.textFile("Data/word.txt")
    rdd1.saveAsTextFile("Data/output")
    sc.stop()
  }

}
