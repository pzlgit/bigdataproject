package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy
 *
 * @author pangzl
 * @create 2022-04-26 19:54
 */
object RDD_GroupBy_Test_1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 小功能：从服务器日志数据apache.log中获取每个时间段访问量？
    val rdd1: RDD[String] = sc.textFile("data/apache.log")
    val rdd2 = rdd1.groupBy(
      line => {
        val words = line.split(" ")
        val time = words(3)
        val hour = time.split(":")(1)
        hour
      }
    )
    val rdd4 = rdd2.mapValues(_.size)
    rdd4.collect().foreach(println)
    sc.stop()
  }
}
