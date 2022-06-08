package com.atguigu.spark.bigdata.scala.chapter04

/**
 * Scala 循环控制之 For 循环
 */
object Scala_For {

  def main(args: Array[String]): Unit = {
    /**
     * For 循环基本语法：
     * for(i <- Range(1,5)){}  从1到5，不包含5
     * for(i <- 1 to 5){}      从1直到5，包含5
     * for(i <- until 5){}     从1到5，不包含5
     */
    for (i <- Range(1, 5)) {
      println(i)
    }
    println("-----------------------------")
    for (i <- 1 to 5) {
      println(i)
    }
    println("-----------------------------")
    for (i <- 1 until 5) {
      println(i)
    }
    println("-----------------------------")

    /**
     * 循环守卫，增加条件来决定是否继续执行循环体
     */
    for (i <- 1 to 5 if i != 3) {
      println(i)
    }
    println("-------------------")

    /**
     * 循环步长
     */
    for (i <- Range(1, 5, 2)) {
      println(i)
    }
    println("-------------")
    for (i <- 1 to 5 by 2) {
      println(i)
    }
    println("-------------")

    /**
     * 循环嵌套
     */
    for (i <- Range(1, 5)) {
      for (j <- Range(1, 5)) {
        println("i=" + i + " " + "j=" + j)
      }
    }
    println("-------------")
    // 改良版循环嵌套
    for (i <- Range(1, 5); j <- Range(1, 5)) {
      println("i=" + i + " " + "j=" + j)
    }
    println("-------------")

    /**
     * 引入变量
     */
    for (i <- Range(1, 5); j = i - 1) {
      println("i=" + i + " " + "j=" + j)
    }
  println("------------------------------")
    /**
     * Scala For 循环返回值 yield
     */
    val result = for (i <- Range(1, 5)) yield {
      i + 1
    }
    println(result)

  }

}