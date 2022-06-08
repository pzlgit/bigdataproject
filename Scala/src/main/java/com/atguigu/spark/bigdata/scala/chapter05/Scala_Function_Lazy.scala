package com.atguigu.spark.bigdata.scala.chapter05

/**
 * Scala 惰性函数 使用
 */
object Scala_Function_Lazy {

  def main(args: Array[String]): Unit = {
    def fun(): Unit = {
      println("fun....")
    }

    lazy val f = fun()
    println("+++++++")
    println(f)
  }

}