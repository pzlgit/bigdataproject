package com.atguigu.bigdata.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 核心环境配置和WordCount实现
 *
 * @author pangzl
 * @create 2022-04-20 15:12
 */
object Spark_WordCount1 {

  def main(args: Array[String]): Unit = {
    //    // 创建Spark配置对象
    //    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    //    // 创建Spark上下文环境对象
    //    val sc: SparkContext = new SparkContext(sparkConf)
    //    // 读取文件获取数据
    //    val lineRDD: RDD[String] = sc.textFile("D:\\WorkShop\\BIgDataProject\\Data\\word.txt")
    //    // 将每行数据按照空格切分
    //    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    //    // 将数据生成元组
    //    val tupleRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    //    // 分组
    //    val wordCount: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    //    // 将数据结果采集到内存中
    //    val result: Array[(String, Int)] = wordCount.collect()
    //    result.foreach(println)
    //    // 关闭Spark连接
    //    sc.stop()

    // 创建Spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建Spark上下文对象
    val sc: SparkContext = new SparkContext(sparkConf)
    // 从文件中读取数据
    val lineRDD: RDD[String] = sc.textFile("data/word.txt")
    // 将每行数据按照空格切分
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    // 将数据构造为元组
    val tupleRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    // 分组聚合
    val wordCount: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)
    // 将数据结果采集到内存中
    val wc: Array[(String, Int)] = wordCount.collect()
    wc.foreach(println(_))
    // 关闭Spark连接
    sc.stop()
  }

}