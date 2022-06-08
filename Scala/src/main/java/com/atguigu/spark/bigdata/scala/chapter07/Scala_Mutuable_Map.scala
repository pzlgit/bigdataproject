package com.atguigu.spark.bigdata.scala.chapter07

import scala.collection.mutable

/**
 * Scala 可变 Map
 *
 * @author pangzl
 * @create 2022-04-17 19:08
 */
object Scala_Mutuable_Map {

  def main(args: Array[String]): Unit = {
    // 创建可变 Map
    val map1 = mutable.Map("a" -> 1, "b" -> 2)
    val map2 = mutable.Map("a" -> 3, "b" -> 4)

    // 添加数据
    //    map1.put("c", 3)
    //    println(map1)

    val map4 = map1 + ("c" -> 100)
    println(map4 eq map1)

    val map5 = map1 += ("d" -> 99)
    println(map5 eq map1)

    // 修改数据，如果数据不存在，则新增数据
    map1.update("a", 78)
    println(map1)

    // 删除数据
    map1.remove("a")
    println(map1)

        val map6 = map1 - "b"
        println(map6)
        println(map6 eq map1)
        val map7 = map1 -= "b"
        println(map7)
        println(map7 eq map1)

      println(map1.clear())

  }

}