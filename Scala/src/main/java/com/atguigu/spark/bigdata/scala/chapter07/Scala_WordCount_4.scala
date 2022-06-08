package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 练习： WordCount2
 *
 * @author pangzl
 * @create 2022-04-17 20:53
 */
object Scala_WordCount_4 {

  def main(args: Array[String]): Unit = {
    val list = List(
      ("Hello World Scala Spark", 4),
      ("Hello World Scala", 3),
      ("Hello World", 2),
      ("Hello", 1),
    )
    // 简化版本
    list.flatMap(
      kv => {
        val wordList = kv._1.split(" ")
        wordList.map((_, kv._2))
      })
      .groupBy(_._1)
      .mapValues(
        list => {
          list.map(_._2).sum
        })
      .toList
      .sortBy(_._2)(Ordering.Int.reverse)
      .take(5)
      .foreach(println)

  }
}