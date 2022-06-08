package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 不可变集Seq
 *
 * @author pangzl
 * @create 2022-04-17 9:05
 */
object Scala_Immutable_Seq {

  def main(args: Array[String]): Unit = {
    // List 创建,采用List伴生对象的apply创建，apply可以省略
    val list1 = List(1, 2, 3, 4)
    // 增加数据
    val list2 = list1 :+ 5
    println(list2)
    val list3 = 0 +: list1
    println(list3)

    // 修改数据生成新的Seq
    val list5 = list1.updated(0, 8)
    println(list5 eq list1)
    println(list5)

    // 不可变List基本操作
    val l1 = List(1, 2, 3, 4)
    val empList = List()
    val nil = Nil
    println(empList eq nil)

    // 创建集合
    val l2 = 1 :: 2 :: 3 :: List()
    val l3 = 5 :: 6 :: 7 :: Nil

    // 添加集合元素
    val l4 = l2 ::: Nil
    println(l4)

    // 连接集合
    val l5 = List.concat(l2, l3)
    println(l5)

    // 创建一个指定重复数量的元素列表
    val l6 = List.fill[String](3)("a")
    println(l6)

  }
}