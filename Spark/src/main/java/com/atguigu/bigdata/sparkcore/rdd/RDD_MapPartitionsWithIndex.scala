package com.atguigu.bigdata.sparkcore.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-25 19:14
 */
object RDD_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((_, index))
      }
    )
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
