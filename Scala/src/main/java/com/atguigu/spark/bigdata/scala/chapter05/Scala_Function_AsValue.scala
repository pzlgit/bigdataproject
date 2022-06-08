package com.atguigu.spark.bigdata.scala.chapter05

/**
 * 函数作为值 使用
 */
object Scala_Function_AsValue {

  def main(args: Array[String]): Unit = {
    // 定义一个无参有返回值的函数
    def fun1(): String = {
      println("--")
      "zhangsan"
    }

    // 函数赋值给变量，并打印fun1函数会执行
    val a = fun1
    println(a)

    // 如果不想让函数执行，只是想访问这个函数本身，可采用特殊符号_进行转化,传参后执行
    val b = fun1 _
    println(b)
    println(b())
    //  当变量类型明确就是一个函数，下划线可以省略，且函数不会执行
    val c: () => String = fun1
    println(c)
    println(c())
  }

}