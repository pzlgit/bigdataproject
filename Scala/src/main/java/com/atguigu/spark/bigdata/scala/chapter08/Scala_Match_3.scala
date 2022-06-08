package com.atguigu.spark.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_3 {

  def main(args: Array[String]): Unit = {
    // Scala 匹配数组
    for (arr <-
           Array(Array(0),
             Array(1, 0),
             Array(0, 1, 0),
             Array(1, 1, 0),
             Array(1, 1, 0, 1),
             Array("hello", 90))) {
      val result = arr match {
        case Array(0) => "0"
        case Array(x, y) => x + "," + y
        case Array(0, _*) => "0开头的数组"
        case _ => "something else"
      }
      println(result)

    }
  }
}