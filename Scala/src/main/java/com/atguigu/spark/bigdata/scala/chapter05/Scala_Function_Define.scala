package com.atguigu.spark.bigdata.scala.chapter05

/**
 * 函数定义
 */
object Scala_Function_Define {

  def main(args: Array[String]): Unit = {
    // 无参无返回值
    def fun1(): Unit = {
      println("无参无返回值")
    }

    fun1()
    println("=======================")

    // 无参有返回值
    def fun2(): String = {
      "无参有返回值"
    }

    println(fun2())
    println("=======================")

    // 有参无返回值
    def fun3(name: String): Unit = {
      println(name)
    }

    fun3("有参无返回值")
    println("=======================")

    // 有参有返回值
    def fun4(name: String): String = {
      name.toUpperCase()
    }

    println(fun4("hh"))
    println("=======================")

    // 多参有返回值
    def fun5(name: String, password: Int): String = {
      name.concat(password.toString)
    }

    println(fun5("scala", 10))
    println("=======================")

    // 多参无返回值
    def fun6(name: String, password: Int): Unit = {
      println(name + password)
    }

  }

}