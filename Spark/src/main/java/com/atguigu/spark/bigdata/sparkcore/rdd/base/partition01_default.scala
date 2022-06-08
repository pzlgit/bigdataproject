package com.atguigu.spark.bigdata.sparkcore.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-25 15:34
 */
object partition01_default {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    rdd1.saveAsTextFile("Data/output3")
    sc.stop()
  }

}
