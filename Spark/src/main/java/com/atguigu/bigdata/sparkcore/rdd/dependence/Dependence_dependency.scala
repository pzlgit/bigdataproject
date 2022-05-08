package com.atguigu.bigdata.sparkcore.rdd.dependence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 依赖关系
 *
 * @author pangzl
 * @create 2022-05-08 11:04
 */
object Dependence_dependency {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // wordCount
    val rdd: RDD[String] = sc.textFile("data/word.txt")

    println(rdd.dependencies)
    println("----------------")

    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))

    println(rdd1.dependencies)
    println("----------------")

    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    println(rdd2.dependencies)
    println("----------------")

    val count: RDD[(String, Int)] = rdd2.reduceByKey(_ + _)

    println(count.dependencies)
    println("----------------")

    count.collect().foreach(println(_))

    sc.stop()
  }
}
