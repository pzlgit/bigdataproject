package com.atguigu.bigdata.sparkcore.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Map 映射 练习
 *
 * @author pangzl
 * @create 2022-04-25 18:44
 */
object RDD_Map_Test {

  def main(args: Array[String]): Unit = {
    // map映射 - 从服务器日志数据apache.log中获取用户请求URL资源路径
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 读取文件内容
    val rdd1 = sc.textFile("apache.log")
    // 使用map映射处理每行数据
    val rdd2 = rdd1.map(_.split(" ")(6))
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
