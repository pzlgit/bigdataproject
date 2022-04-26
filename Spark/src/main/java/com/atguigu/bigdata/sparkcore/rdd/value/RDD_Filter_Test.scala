package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Filter
 *
 * @author pangzl
 * @create 2022-04-26 20:10
 */
object RDD_Filter_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //小功能：从服务器日志数据apache.log中获取2015年5月17日的请求路径？
    val rdd1 = sc.textFile("data/apache.log")
    val rdd2 = rdd1.filter(data => {
      val words = data.split(" ")
      words(3).startsWith("17/05/2015")
    }).map(
      data => {
        val words = data.split(" ")
        words(6)
      }
    )
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
