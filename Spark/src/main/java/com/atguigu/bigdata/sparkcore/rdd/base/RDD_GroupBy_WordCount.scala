package com.atguigu.bigdata.sparkcore.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupby 分组
 *
 * @author pangzl
 * @create 2022-04-25 19:23
 */
object RDD_GroupBy_WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd1 = sc.textFile("Data/word.txt")
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_, 1))
    val rdd4 = rdd3.groupBy(_._1)
    val rdd5 = rdd4.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    rdd5.collect().foreach(println(_))
    sc.stop()
  }
}
