package com.atguigu.bigdata.sparkcore.rdd.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * intersection 交集
 *
 * @author pangzl
 * @create 2022-04-26 21:06
 */
object RDD_Intersection {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建两个RDD，求两个RDD的交集。
    val rdd1 = sc.makeRDD(1 to 4)
    val rdd2 = sc.makeRDD(4 to 8)
    val rdd3 = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println(_))
    sc.stop()
  }
}
