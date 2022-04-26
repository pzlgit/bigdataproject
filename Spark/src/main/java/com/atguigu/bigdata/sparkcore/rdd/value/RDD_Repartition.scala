package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Repartition
 *
 * @author pangzl
 * @create 2022-04-26 20:44
 */
object RDD_Repartition {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // repartition 重新分区 执行 shuffle
    // 创建一个4个分区的RDD，对其重新分区。
    val rdd1 = sc.makeRDD(1 to 6, 3)
    val rdd2 = rdd1.repartition(6)
    rdd2.mapPartitionsWithIndex(
      (index,datas) =>{
        datas.map((index,_))
      }
    ).collect().foreach(println(_))


    sc.stop()
  }
}
