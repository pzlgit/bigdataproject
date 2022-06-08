package com.atguigu.spark.bigdata.scala.chapter10

/**
 * Scala 隐式转换
 *
 * @author pangzl
 * @create 2022-04-19 18:57
 */
object Scala_Implicit_1 {

  def main(args: Array[String]): Unit = {
    // 定义隐式转化参数 和 隐式转化变量
    def transform(implicit x: Double) = {
      x.toInt
    }

    implicit val d: Double = 2.3
    println(transform)
  }
}
