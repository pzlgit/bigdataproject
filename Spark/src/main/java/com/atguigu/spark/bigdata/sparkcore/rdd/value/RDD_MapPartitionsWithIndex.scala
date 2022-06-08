package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitionsWithIndex
 *
 * @author pangzl
 * @create 2022-04-26 19:31
 */
object RDD_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD。
    val rdd1 = sc.makeRDD(1 to 4, 2)
    val rdd2 = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((index, _))
      }
    )
    rdd2.collect().foreach(println(_))
    sc.stop()
  }
}
