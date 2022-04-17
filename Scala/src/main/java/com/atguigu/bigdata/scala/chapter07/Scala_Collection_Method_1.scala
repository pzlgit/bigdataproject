package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 常用方法1
 *
 * @author pangzl
 * @create 2022-04-17 19:59
 */
object Scala_Collection_Method_1 {

  def main(args: Array[String]): Unit = {
    // Scala 常用方法 之 衍生方法
    val list = List(1, 2, 3, 4)
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)

    // 获取集合头
    println(list.head)
    // 获取集合尾
    println(list.tail)
    // 集合尾迭代器
    val tails: Iterator[List[Int]] = list.tails
    while (tails.hasNext) {
      println(tails.next())
    }

    //获取集合初始值
    println(list.init)

    // 获取集合最后一个元素
    println(list.last)

    // 集合并集、交集、差集
    println(list.union(list2))
    println(list.intersect(list2))
    println(list.diff(list2))

    // 拉链，将两个数据集中相同位置的数据拉在一起，形成对偶元组
    println(list.zip(list1))
    // 数据索引拉链
    println(list.zip(list1))

    // 切分集合
    println(list.splitAt(2))

    println("=====================")
    // 滑动窗口
    val iterator = list.sliding(2)
    while (iterator.hasNext){
      println(iterator.next())
    }

    println("----------")
    val iterator1 = list.sliding(2, 2)
    while (iterator1.hasNext){
      println(iterator1.next())
    }


  }
}