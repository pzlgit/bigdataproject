package com.atguigu.spark.bigdata.scala.chapter11

/**
 *
 * Scala 泛型
 *
 * @author pangzl
 * @create 2022-04-20 18:42
 */
object Scala_FanXing {

  def main(args: Array[String]): Unit = {
    new Test[List[Int]]
  }
}

class Test[T] {
  private var i: List[T] = Nil
}