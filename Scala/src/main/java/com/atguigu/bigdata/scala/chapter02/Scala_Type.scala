package com.atguigu.bigdata.scala.chapter02

/**
 * Scala 类型转换
 */
object Scala_Type {

  def main(args: Array[String]): Unit = {
    // Scala 中没有基本数据类型，有任意值对象(AnyVal) 和 任意引用对象(AnyRef)
    val age: Int = 10
    val name: String = "zhangsan"

    /**
     * 自动类型转换
     */
    val b: Byte = 10
    val s: Short = b
    val i: Int = b
    val l: Long = i

    /**
     * 如下的程序运行结果正确
     */
    val c : Char = 'A' + 1
    println(c)

    /**
     * 强制类型转换
     */
    val x : Int = 100
    val y : Short = x.toShort
    println(y)

    /**
     * 字符串类型转换
     */
    println(x.toString + "100")

  }

}