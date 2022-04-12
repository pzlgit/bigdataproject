package com.atguigu.bigdata.scala.chapter03

/**
 * Scala 运算符
 */
object Scala_Oper {

  def main(args: Array[String]): Unit = {
    /**
     * 算数运算符
     */
    val a: Int = 10
    val b: Int = 5
    println(a .+(b))
    print(a - b)
    println(a * b)
    println(a / b)
    println(a % b)

    /**
     * 关系运算符
     */
    val x: String = new String("shangsan")
    val y: String = new String("shangsan")
    println(x == y)
    println(x equals y)
    println(x eq y) // 比较引用地址

    /**
     * 赋值运算符
     */
    var c: Int = 30
    c += 30
    println(c)

  }

}