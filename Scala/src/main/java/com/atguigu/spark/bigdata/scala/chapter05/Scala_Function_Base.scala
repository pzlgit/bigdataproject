package com.atguigu.spark.bigdata.scala.chapter05

/**
 * 函数基本定义及了解
 */
object Scala_Function_Base {

  def main(args: Array[String]): Unit = {
    /**
     * 基本函数编程
     * 修饰符 def 函数名称(参数列表) : 返回值类型 = {
     * 函数体
     * }
     */
    def testPrint(name: String): Unit = {
      println(name)
    }

    testPrint("test...")
  }

}