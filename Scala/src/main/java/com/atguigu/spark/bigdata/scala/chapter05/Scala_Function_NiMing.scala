package com.atguigu.spark.bigdata.scala.chapter05

/**
 * 匿名函数 使用
 */
object Scala_Function_NiMing {

  def main(args: Array[String]): Unit = {
    //    def fun(f: Int => Int) = {
    //      f(10)
    //    }
    //
    //    def test(x: Int): Int = {
    //      x * 20
    //    }
    //
    //    val result = fun(
    //      _ * 20
    //    )
    //    println(result)

    // 实现一个计算器的小功能
    def calculate(x: Int, f: (Int, Int) => Int, y: Int) = {
      f(x, y)
    }
    def sum(x: Int, y: Int): Int = {
      x + y
    }
    val result = calculate(1, sum, 2)
    println(result)
    // 匿名函数实现
    println(calculate(1, (x: Int, y: Int) => {x + y}, 2))
    println(calculate(1, (x: Int, y: Int) => x + y, 2))
    println(calculate(1, (x, y) => x + y, 2))
    println(calculate(1, _ + _, 2))
  }

}