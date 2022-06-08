package com.atguigu.spark.bigdata.scala.chapter07

import scala.collection.mutable

/**
 * Scala 队列
 *
 * @author pangzl
 * @create 2022-04-17 19:52
 */
object Scala_Queue {

  def main(args: Array[String]): Unit = {
    // 创建可变队列
    val que: mutable.Queue[String] = new mutable.Queue[String]()
    // 队列增加数据
    que.enqueue("1", "2", "3")

    val que1 = que += "4"
    println(que1 eq (que))

   println(que.dequeue())
   println(que.dequeue())
   println(que.dequeue())

  }
}