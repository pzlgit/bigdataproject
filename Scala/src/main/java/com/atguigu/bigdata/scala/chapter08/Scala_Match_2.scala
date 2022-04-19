package com.atguigu.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_2 {

  def main(args: Array[String]): Unit = {
    // Scala 匹配类型
    def describe(x: Any) = x match {
      case i: Int => "Int"
      case arr: Array[Int] => "Array"
      case str: String => "String"
      case list: List[_] => "List"
      case something => "something else"
    }

    println(describe(List("2")))
  }
}