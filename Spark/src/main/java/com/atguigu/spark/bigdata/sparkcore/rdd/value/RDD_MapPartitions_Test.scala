package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitions
 *
 * @author pangzl
 * @create 2022-04-26 19:22
 */
object RDD_MapPartitions_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD，4个元素，2个分区，使每个元素*2组成新的RDD
    val rdd1 = sc.makeRDD(1 to 4, 2)
    // 小功能：获取每个数据分区的最大值？
    val rdd2 = rdd1.mapPartitions(
      datas => {
        List(datas.max).iterator
      }
    )
    rdd2.collect().foreach(println(_))
    sc.stop()
  }
}
