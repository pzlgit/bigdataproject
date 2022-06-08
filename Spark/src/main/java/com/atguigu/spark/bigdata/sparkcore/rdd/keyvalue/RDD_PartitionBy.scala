package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * PartitionBy
 *
 * @author pangzl
 * @create 2022-04-26 21:25
 */
object RDD_PartitionBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // partitionBy 按照k重新分区
    // 创建一个3个分区的RDD，对其重新分区。
    val rdd1 = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    val rdd2 = rdd1.partitionBy(new HashPartitioner(2))
    rdd2.mapPartitionsWithIndex(
      (index,datas) =>{
        datas.map((index,_))
      }
    ).collect().foreach(println(_))
    sc.stop()
  }
}
