package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 并行
 *
 * @author pangzl
 * @create 2022-04-17 19:57
 */
object Scala_Para {

  def main(args: Array[String]): Unit = {
    // 串行集合
    val result1 = (0 to 100).map { x => Thread.currentThread.getName }
    // 并行集合
    val result2 = (0 to 100).par.map { x => Thread.currentThread.getName }
    println(result1)
    println(result2)
  }
}