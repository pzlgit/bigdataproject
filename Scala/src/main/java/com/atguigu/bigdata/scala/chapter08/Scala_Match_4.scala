package com.atguigu.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_4 {

  def main(args: Array[String]): Unit = {
    // Scala 匹配列表
    for (list <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0), List(88))) {
      val result = list match {
        case List(0) => "0" // 匹配List(0)
        case List(x, y) => x + "," + y // 匹配有两个元素的List
        case List(0, _*) => "0 ..." // 匹配以0开头的集合
        case _ => "something else"
      }
      println(result)
    }
    // 1-2-List(5, 6, 7)
    val list: List[Int] = List(1, 2, 5, 6, 7)
    list match {
      case first :: second :: rest => println(first + "-" + second + "-" + rest)
      case _ => println("something else")
    }
  }

}