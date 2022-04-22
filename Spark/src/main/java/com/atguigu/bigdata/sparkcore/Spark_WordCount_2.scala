package com.atguigu.bigdata.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark WordCount 再测试
 *
 * @author pangzl
 * @create 2022-04-22 18:56
 */
object Spark_WordCount_2 {

  def main(args: Array[String]): Unit = {
    // 创建Spark配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建Spark上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 读取文件中的数据
    val lineRDD: RDD[String] = sc.textFile("Data/word.txt")
    // 将每一行数据按照空格切分
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    // 将数据转化为元组
    val tupleRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    // 分组聚合求wordCount
    val wordCount: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    // 收集
    val result: Array[(String, Int)] = wordCount.collect()
    // 打印
    result.foreach(println(_))
    // 关闭连接
    sc.stop()
  }
}
