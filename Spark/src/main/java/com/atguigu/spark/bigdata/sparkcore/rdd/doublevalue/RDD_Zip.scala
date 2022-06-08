package com.atguigu.spark.bigdata.sparkcore.rdd.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * zip 拉链操作
 *
 * @author pangzl
 * @create 2022-04-26 21:16
 */
object RDD_Zip {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 2, 3), 3)
    val rdd2 = sc.makeRDD(List("a", "b", "c"), 3)
    // zip
    rdd2.zip(rdd1).collect().foreach(println(_))
    // 创建新rdd，与rdd1,rdd2元素个数不同
    val rdd3 = sc.makeRDD(List(1, 2), 3)
//    rdd3.zip(rdd1).collect().foreach(println(_))
    // 创建新rdd，与rdd1,rdd2分区数不同
    val rdd4 = sc.makeRDD(List(1, 2,3), 4)
    rdd4.zip(rdd1).collect().foreach(println(_))
    sc.stop()
  }
}
