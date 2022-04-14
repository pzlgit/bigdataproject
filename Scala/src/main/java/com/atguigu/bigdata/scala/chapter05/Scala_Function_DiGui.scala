package com.atguigu.bigdata.scala.chapter05

/**
 * Scala 递归函数 使用
 */
object Scala_Function_DiGui {

  def main(args: Array[String]): Unit = {
    // 求5的阶乘
    def fun(i: Int): Int = {
      if (i <= 1) {
        i
      } else {
        i * fun(i - 1)
      }
    }

    val result = fun(5)
    println(result)
  }

}