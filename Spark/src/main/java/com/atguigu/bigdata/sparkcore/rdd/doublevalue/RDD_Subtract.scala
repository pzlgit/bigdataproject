package com.atguigu.bigdata.sparkcore.rdd.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * subtract 差集
 *
 * @author pangzl
 * @create 2022-04-26 21:12
 */
object RDD_Subtract {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // subtract 差集
    val rdd1 = sc.makeRDD(1 to 4)
    val rdd2 = sc.makeRDD(4 to 8)
    rdd1.subtract(rdd2).collect().foreach(println(_))
    sc.stop()
  }
}
