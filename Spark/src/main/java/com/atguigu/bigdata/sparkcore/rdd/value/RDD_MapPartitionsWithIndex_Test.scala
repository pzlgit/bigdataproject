package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * MapPartitionsWithIndex
 *
 * @author pangzl
 * @create 2022-04-26 19:31
 */
object RDD_MapPartitionsWithIndex_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 小功能：获取第二个数据分区的数据？
    val rdd1 = sc.makeRDD(1 to 4, 2)
    val rdd2 = rdd1.mapPartitionsWithIndex(
      (index, datas) => {
        if (index == 1) {
          datas
        } else {
          List().iterator
        }
      }
    )
    rdd2.collect().foreach(println(_))
    sc.stop()
  }
}
