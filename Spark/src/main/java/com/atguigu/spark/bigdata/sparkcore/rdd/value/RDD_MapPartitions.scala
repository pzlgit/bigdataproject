package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitions
 *
 * @author pangzl
 * @create 2022-04-26 19:22
 */
object RDD_MapPartitions {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD，4个元素，2个分区，使每个元素*2组成新的RDD
    val rdd1 = sc.makeRDD(1 to 4, 2)
    val rdd2 = rdd1.mapPartitions(
      iterator => {
        println("********")
        iterator.map(_ * 2)
      }
    )
    rdd2.collect().foreach(println(_))

    println("++++++++++++++++")
    val rdd3 = rdd1.mapPartitions(
      datas => {
        datas.filter(_ % 2 == 0)
      }
    )
    rdd3.collect().foreach(println(_))
    sc.stop()
  }
}
