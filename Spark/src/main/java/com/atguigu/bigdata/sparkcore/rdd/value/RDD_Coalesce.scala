package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Coalesce
 *
 * @author pangzl
 * @create 2022-04-26 20:37
 */
object RDD_Coalesce {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // Coalesce合并分区
    // 不执行Shuffle方式
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4,5,6), 3)
    // 缩减分区为2
    val rdd2 = rdd1.coalesce(2)
    // 查看对应分区数据
    rdd2.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((index,_))
      }
    ).collect().foreach(println(_))
    // 执行shuffle方式
    val rdd3 = rdd1.coalesce(2, true)
    rdd3.mapPartitionsWithIndex(
      (index,datas) =>{
        datas.map((index,_))
      }
    ).collect().foreach(println(_))
    sc.stop()
  }
}
