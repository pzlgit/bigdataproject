package com.atguigu.bigdata.scala.chapter05

/**
 * Scala 函数柯里化
 */
object Scala_Function_Curry {

  def main(args: Array[String]): Unit = {

    def fun(x: Int)(y: Int): Unit = {
      println("x=" + x)
      println("y=" + y)
    }

    val f: Int => Unit = fun(1)
    f(2)

    fun(3)(4)
  }

}