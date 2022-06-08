package com.atguigu.spark.bigdata.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 20:39
 */
object RDD_Collect {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // collect 创建一个RDD，并将RDD内容收集到Driver端打印。
    val rdd = sc.makeRDD(1 to 4)
    rdd.collect().foreach(println(_))

    sc.stop()
  }
}
