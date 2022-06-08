package com.atguigu.spark.bigdata.scala.chapter05

/**
 * 函数作为参数 使用
 */
object Scala_Function_AsParam {


  def main(args: Array[String]): Unit = {

    def sum(x: Int, y: Int): Int = {
      x + y
    }

    def test(f: (Int, Int) => Int) = {
      f(10, 21)
    }

    test(sum)
    println(test(sum))
  }

}