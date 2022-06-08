package com.atguigu.spark.bigdata.scala.chapter07

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

/**
 * Scala 集合
 *
 * @author pangzl
 * @create 2022-04-17 9:33
 */
object Scala_Seq {

  def main(args: Array[String]): Unit = {
    // 可变集合与不可变集合转换
    val list1 = List(1, 2, 3, 4)
    val list2 = ListBuffer(5, 4, 6, 8)

    // 将可变集合转化为不可变集合
    val l1: immutable.Seq[Int] = list2.toList
    // 将不可变集合转化为可变集合
    val bu: mutable.Seq[Int] = list1.toBuffer
  }
}
