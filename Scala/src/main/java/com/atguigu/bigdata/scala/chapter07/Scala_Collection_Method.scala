package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 常用方法
 *
 * @author pangzl
 * @create 2022-04-17 19:59
 */
object Scala_Collection_Method {

  def main(args: Array[String]): Unit = {
    // Scala 常用方法 之 常用方法
    val list = List(1, 2, 3, 4)

    // 集合长度
    println(list.length)
    println(list.size)

    // 判断集合是否为空
    println(list.isEmpty)
    // 集合迭代器
    val iterator = list.iterator
    while (iterator.hasNext) {
      println(iterator.next())
    }
    // 循环遍历集合
    list.foreach(println)

    // 将集合转化为字符串
    println(list.mkString(","))
    println(list.contains(1))

    // 取集合的前几个元素
    println(list.take(2))
    println(list.takeRight(2))

    // 查找元素
    val maybeInt = list.find(_ % 2 == 0)
    println(list.find(_ % 2 == 0))

    // 丢弃前几个元素
    println(list.drop(2))
    println(list.dropRight(2))

    // 反转集合
    println(list.reverse)


  }
}