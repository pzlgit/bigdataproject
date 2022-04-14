package com.atguigu.bigdata.scala.chapter05

/**
 * 函数作为返回值 使用
 */
object Scala_Function_AsReturnParam {

  def main(args: Array[String]): Unit = {
    def outer(x: Int) = {
      def middle(f: (Int, Int) => Int) = {
        def inner(y: Int) = {
          f(x, y)
        }
        inner _
      }
      middle _
    }

    def sum(x: Int, y: Int): Int = {
      x + y
    }

    val middle = outer(10)
    val inner = middle(sum)
    val result = inner(20)
    println(result)

    println(outer(10)(_ + _)(20))
  }

}