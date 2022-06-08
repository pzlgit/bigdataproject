package com.atguigu.spark.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match {

  def main(args: Array[String]): Unit = {
    val a: Int = 10
    val b: Int = 20
    val operator: Char = 'd'

    // 定义Scala模式匹配
    var result = operator match {
      case '+' => a + b
      case '-' => a - b
      case '*' => a * b
      case '/' => a / b
      case _ => "illegal"
    }

    println(result)
  }
}