package com.atguigu.spark.bigdata.scala.chapter04

/**
 * Scala 分支控制
 */
object Scala_Flow {

  def main(args: Array[String]): Unit = {
    /**
     * 单分支控制
     */
    val flag: Boolean = true
    if (flag) {
      println("true")
    }

    /**
     * 双分支控制
     */
    val flag1: Boolean = false
    if (flag1) {
      println("flag1")
    } else {
      println("other")
    }

    /**
     * 多分支控制
     * 输入年龄，如果年龄小于18岁，输出“童年”。如果年龄大于等于18且小于等于30，
     * 输出“青年”，如果年龄大于30小于等于50，输出”中年”，否则输出“老年”。
     */
    val age: Int = 10
    if (age < 18) {
      println("童年")
    } else if (age <= 30) {
      println("青年")
    } else if (age <= 50) {
      println("中年")
    } else {
      println("老年")
    }
    // Scala中的表达式都是有返回值的，返回值默认是满足条件的最后一行的执行结果

    val result = if (age < 18) {
      "童年"
    } else if (age <= 30) {
      "青年"
    } else if (age <= 50) {
      "中年"
    } else {
      "老年"
    }
    println(result)
  }

}