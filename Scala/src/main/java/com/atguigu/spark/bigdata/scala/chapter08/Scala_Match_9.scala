package com.atguigu.spark.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_9 {

  def main(args: Array[String]): Unit = {
    // 模式匹配 循环匹配
    val map = Map(
      ("a", 1), ("b", 2), ("c", 3)
    )
    for ((k, v) <- map) {
      println(k + "=" + v)
    }

    // 输出满足条件的指定k,v
    for ((k, 1) <- map) {
      println(k + "=" + 1)
    }
  }

}