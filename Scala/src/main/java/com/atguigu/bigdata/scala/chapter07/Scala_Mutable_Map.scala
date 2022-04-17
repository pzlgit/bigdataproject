package com.atguigu.bigdata.scala.chapter07

import scala.collection.mutable

/**
 * Scala 可变 Map
 *
 * @author pangzl
 * @create 2022-04-17 19:20
 */
object Scala_Mutable_Map {

  def main(args: Array[String]): Unit = {
    val m1 = mutable.Map("a" -> 1, "b" -> 2, "c" -> 3)
    val m2 = mutable.Map("d" -> 4, "e" -> 5, "f" -> 6)

    // Map数据转化为Set 和 List
    val list = m1.toList
    println(list.mkString(","))
    val set = m1.toSet
    println(set.mkString(","))

    val seq = m1.toSeq
    println(seq.mkString(","))

    val array = m1.toArray
    println(array.mkString(","))

    println(m1.get("a"))
    println(m1.getOrElse("a",0))

    println(m1.keys)
    println(m1.keySet)
    println(m1.keysIterator)
    println(m1.values)
    println(m1.valuesIterator)
  }
}