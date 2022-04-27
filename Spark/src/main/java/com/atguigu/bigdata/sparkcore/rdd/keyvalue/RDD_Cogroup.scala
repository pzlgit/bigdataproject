package com.atguigu.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:50
 */
object RDD_Cogroup {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // cogroup 类似全连接，但是是在同一个rdd中对key进行聚合
    // 1 创建第一个RDD
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1,"a"),(2,"b"),(3,"c")))
    // 2 创建第二个RDD
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1,4),(2,5),(4,6)))
    // 操作
    rdd1.cogroup(rdd2).collect().foreach(println)
    sc.stop()
  }
}
