package com.atguigu.bigdata.sparkcore.rdd.doublevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * union 并集
 *
 * @author pangzl
 * @create 2022-04-26 21:09
 */
object RDD_Union {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // union 求两个rdd的并集
    val rdd1 = sc.makeRDD(1 to 4)
    val rdd2 = sc.makeRDD(3 to 6)
    val rdd3 = rdd1.union(rdd2)

    rdd3.collect().foreach(println(_))
    sc.stop()
  }
}
