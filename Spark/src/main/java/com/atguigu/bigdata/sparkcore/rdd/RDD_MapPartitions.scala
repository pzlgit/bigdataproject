package com.atguigu.bigdata.sparkcore.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitions 以分区为单位执行Map
 *
 * @author pangzl
 * @create 2022-04-25 18:44
 */
object RDD_MapPartitions {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD,4个元素，2个分区，使每个元素*2组成新的RDD
    val rdd1 = sc.makeRDD(1 to 4, 2)
    //    val rdd2 = rdd1.mapPartitions(
    //      datas => {
    //        println("========")
    //        datas.map(_ * 2)
    //      }
    //    )
    //    rdd2.saveAsTextFile("Data/output1")
    // 每个分区中过滤偶数
    //    val rdd2 = rdd1.mapPartitions(
    //      datas => {
    //        datas.filter(_ % 2 == 0)
    //      }
    //    )
    //    rdd2.collect().foreach(println(_))
    // 获取每个数据分区的最大值
    val rdd2 = rdd1.mapPartitions(
      datas => {
        List(datas.max).iterator
      }
    )
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
