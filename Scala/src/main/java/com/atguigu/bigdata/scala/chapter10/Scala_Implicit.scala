package com.atguigu.bigdata.scala.chapter10

/**
 * Scala 隐式转换
 *
 * @author pangzl
 * @create 2022-04-19 18:57
 */
object Scala_Implicit {

  def main(args: Array[String]): Unit = {
    // 定义隐式转换函数
    implicit def transform(d: Double): Int = {
      d.toInt
    }

    val d: Double = 2.3
    val i: Int = d
    println(i)
  }
}
