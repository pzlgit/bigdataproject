package com.atguigu.spark.bigdata.sparkcore.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark 核心环境配置和WordCount实现
 *
 * @author pangzl
 * @create 2022-04-20 15:12
 */
object Spark_WordCount {

  def main(args: Array[String]): Unit = {
    //    // 创建Spark配置对象
    //    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    //    // 创建Spark上下文环境对象
    //    val sc: SparkContext = new SparkContext(sparkConf)
    //    // 读取文件获取数据
    //    val lineRDD: RDD[String] = sc.textFile("D:\\WorkShop\\BIgDataProject\\Data\\word.txt")
    //    // 将每行数据按照空格切分
    //    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    //    // 将切分后的数据分组
    //    val groupMap: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)
    //    // 分组数据计算单词出现的频率
    //    val wordCount: RDD[(String, Int)] = groupMap.mapValues(_.size)
    //    wordCount.foreach(println)
    //    // 关闭Spark连接
    //    sc.stop()
    // 构建Spark配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 构建Spark上下文环境对象SparkContext
    val sc: SparkContext = new SparkContext(sparkConf)
    // 从文件中读取数据
    val lineRDD: RDD[String] = sc.textFile("data/word.txt")
    // 将每行数据按照空格切分
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    // 数据分组
    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)
    // 统计单词出现的频率
    val wordCount: RDD[(String, Int)] = groupRDD.mapValues(_.size)
    // 打印结果
    wordCount.foreach(println)
    // 关闭Spark连接
    sc.stop()
  }

}
