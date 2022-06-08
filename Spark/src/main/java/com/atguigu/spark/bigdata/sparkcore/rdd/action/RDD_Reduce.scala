package com.atguigu.spark.bigdata.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 20:36
 */
object RDD_Reduce {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // reduce  创建一个RDD，将所有元素聚合得到结果。
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val result = rdd.reduce(_ + _)
    println(result)
    sc.stop()
  }
}
