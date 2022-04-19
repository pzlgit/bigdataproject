package com.atguigu.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_10 {

  def main(args: Array[String]): Unit = {
    val list = List(
      ("a", 1), ("b", 2), ("c", 3)
    )
    val list1 = list.map {
      case (k, v) => {
        (k, v * 2)
      }
    }
    println(list1)
  }

}