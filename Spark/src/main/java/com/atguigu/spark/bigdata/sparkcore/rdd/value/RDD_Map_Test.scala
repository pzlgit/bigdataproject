package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Map 从服务器日志数据apache.log中获取用户请求URL资源路径？
 *
 * @author pangzl
 * @create 2022-04-26 19:10
 */
object RDD_Map_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 从服务器日志数据apache.log中获取用户请求URL资源路径？
    val rdd1 = sc.textFile("data/apache.log")
    // 获取用户请求URL资源路径
    val rdd2 = rdd1.map(
      line => {
        val words = line.split(" ")
        words(6)
      }
    )
    // 收集打印输出
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
