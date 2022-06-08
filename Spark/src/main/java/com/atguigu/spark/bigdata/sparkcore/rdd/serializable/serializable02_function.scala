package com.atguigu.spark.bigdata.sparkcore.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 序列化方法和属性
 *
 * @author pangzl
 * @create 2022-05-08 10:47
 */
object serializable02_function {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // 创建RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    // 创建一个Search
    val search = new Search("hello")

    // 函数传递，打印 Error:Task not serializable
    search.getMatch1(rdd).collect().foreach(println(_))

    // 属性传递，打印 ERROR: ask not serializable
    search.getMatch2(rdd).collect().foreach(println(_))

    sc.stop()
  }

 case  class Search(query: String)  {

    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(this.isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      rdd.filter(x => {
        x.contains(this.query)
      })
    }

  }
}
