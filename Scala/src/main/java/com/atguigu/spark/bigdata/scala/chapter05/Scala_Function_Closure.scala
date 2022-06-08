package com.atguigu.spark.bigdata.scala.chapter05

/**
 * 函数闭包 使用
 */
object Scala_Function_Closure {

  def main(args: Array[String]): Unit = {
    def outer(x: Int) = {
      def inner(y: Int) = {
        x + y
      }
      inner _
    }
    val inner = outer(10)
    val result = inner(20)
    println(result)
  }

}