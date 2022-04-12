package com.atguigu.bigdata.scala.chapter05

/**
 * 函数参数
 */
object Scala_Function_Param {

  def main(args: Array[String]): Unit = {
    // 可变参数
    def fun1(names: String*): Unit = {
      println(names)
    }

    fun1("zhang", "zhou", "li")

    /**
     * 函数参数默认值
     */
    def fun2(name: String, password: String = "00000"): Unit = {
      println(name + password)
    }

    fun2("zhangsan", "12345")
    fun2("lisi")

    /**
     * 带名函数
     */
    def fun3(name: String = "default", age: Int): Unit = {
      println(name + age)
    }

    fun3(age = 1)
  }

}