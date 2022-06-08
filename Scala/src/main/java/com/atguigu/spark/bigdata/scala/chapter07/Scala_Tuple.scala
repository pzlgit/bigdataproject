package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 元组
 *
 * @author pangzl
 * @create 2022-04-17 19:45
 */
object Scala_Tuple {

  def main(args: Array[String]): Unit = {
    // 创建元组
    val tuple1 = (1, "zhangsan", 30)
    val tuple2 = (1, "zhangsan", 30)
    // 根据顺序号访问元组的数据，最大的顺序号等同于元素的个数
    println(tuple1._1)
    println(tuple1._2)
    println(tuple1._3)

    // 元组的迭代器
    val iterator = tuple1.productIterator
    while (iterator.hasNext) {
      println(iterator.next())
    }

    // 根据索引访问元素
    println(tuple1.productElement(0))

    // 如果元组的元素个数只有两个，那么我们称之为对偶元组，也成为键值对
    val kv: (String, Int) = ("a", 1)
    val kv1: (String, Int) = "a" -> 1
    println(kv eq kv1)

    // 使用元组构建Map
    val map = Map(
      "a" -> 1, "b" -> 2
    )
    println(map)


  }

}