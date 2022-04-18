package com.atguigu.bigdata.scala.chapter07

import scala.collection.immutable

/**
 * Scala 练习： WordCount2
 *
 * @author pangzl
 * @create 2022-04-17 20:53
 */
object Scala_WordCount_2 {

  def main(args: Array[String]): Unit = {
    val list = List(
      ("Hello Scala", 4),
      ("Hello Spark", 3),
      ("Hello Spark", 2)
    )
    val list1: immutable.Seq[String] = list.map(
      kv => {
        (kv._1 + " ") * kv._2
      }
    )
    val wordList = list1.flatMap(
      str => {
        str.split(" ")
      }
    )
    val tupleList = wordList.map((_, 1))
    val groupList = tupleList.groupBy(_._1)
    val wordCount = groupList.mapValues(_.size)
    val orderList = wordCount.toList.sortBy(_._2)(Ordering.Int.reverse)
    println(orderList)
    println(orderList.take(2))
  }
}