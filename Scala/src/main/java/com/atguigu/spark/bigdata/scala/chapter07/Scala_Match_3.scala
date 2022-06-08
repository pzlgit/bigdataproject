package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 9:53
 */
object Scala_Match_3 {

  def main(args: Array[String]): Unit = {
    // Scala 循环匹配
    val map = Map(
      ("a" -> 1), ("b" -> 2), ("c" -> 3)
    )
    for ((k, v) <- map) {
      println(k + "=" + v)
    }
    println("=====")
    for ((k,1) <- map) {
      println(k + "=" + 1)
    }

    // 函数参数

  }
}