package com.atguigu.bigdata.scala.chapter07

import scala.collection.mutable

/**
 * Scala 可变Set
 *
 * @author pangzl
 * @create 2022-04-17 9:46
 */
object Scala_Mutable_Set {

  def main(args: Array[String]): Unit = {
    val s1 = mutable.Set(1, 2, 3, 4)
    val s2 = mutable.Set(5, 6, 7, 8)

    s1.add(5)
    s1.update(5, true)
    s1.remove(5)
    println(s1)


    println("================")
    //交集
    val s4 = s1 & s2
    // 差集
    val s5 = s1 &~ s2
    println(s4)
    println(s5)
  }

}