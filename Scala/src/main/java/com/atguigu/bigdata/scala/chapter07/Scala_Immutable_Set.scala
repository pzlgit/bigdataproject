package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 不可变Set
 *
 * @author pangzl
 * @create 2022-04-17 9:46
 */
object Scala_Immutable_Set {

  def main(args: Array[String]): Unit = {
    // 创建不可变Set 无序不可重复
    val s1 = Set(1, 1, 2, 3, 4, 5, 6, 7)
    val s2 = Set(8, 8, 9, 9)

    // 增加数据
    val s3 = s1 + 8 + 9
    println(s3)
    val s4 = s1.+(6, 6, 6, 6)
    println(s4)
    println("============")
    // 删除数据
    val s5 = s1 - 1 - 2
    println(s5)

    // 集合相加
    val s6 = s1 ++: s2
    val s7 = s1 ++ s2
    println(s6 eq s1)
  }

}