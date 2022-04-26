package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Map
 *
 * @author pangzl
 * @create 2022-04-26 19:10
 */
object RDD_Map {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建RDD，两个分区
    val rdd1 = sc.makeRDD(1 to 4, 2)
    // 每个元素 * 2
    //    val rdd2 = rdd1.map(
    //      num => {
    //        println(num)
    //        num * 2
    //      }
    //    )
    val rdd2 = rdd1.map(_ * 2)
    // 打印输出
    rdd2.collect().foreach(println)
    //    rdd1.saveAsTextFile("data/output1")
    //    rdd2.saveAsTextFile("data/output2")
    sc.stop()
  }
}
