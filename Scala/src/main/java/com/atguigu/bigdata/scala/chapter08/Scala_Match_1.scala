package com.atguigu.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_1 {

  def main(args: Array[String]): Unit = {
    // Scala 匹配常量
    def describe(x: Any) = x match {
      case 5 => "five Int"
      case "scala" => "String scala"
      case true => "true"
      case _ => "others"
    }

    println(describe("scal1a"))
  }
}