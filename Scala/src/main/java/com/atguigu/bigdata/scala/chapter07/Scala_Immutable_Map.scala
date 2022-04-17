package com.atguigu.bigdata.scala.chapter07

/**
 * Scala Map ： Map 映射是一种可迭代的键值对结构，数据无序且不能重复，所有的值都可以通过键来获取
 *
 * @author pangzl
 * @create 2022-04-17 18:49
 */
object Scala_Immutable_Map {

  def main(args: Array[String]): Unit = {
    // 不可变 Map
    val map1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val map2 = Map("d" -> 4, "e" -> 5, "f" -> 6)
    // 添加数据
    val map3 = map1 + ("h" -> 5)
    println(map3)
    println(map3 eq map1)
    // 删除数据
    val map4 = map1 - ("h")
    println(map4.mkString(", "))

    println("===============")
    // 集合相加
    val map5 = map1 ++ map2
    println(map5.mkString(","))
    println(map5 eq map1)

    val map6 = map1 ++: map2
    println(map6 eq map1)

    // 修改数据会生成新的Map
    val map7 = map1.updated("a", 0)
    println(map7.mkString(","))

    println("================")
    // 不可变Map基本操作
    val m1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
    val m2 = Map("d" -> 4, "e" -> 5, "f" -> 6)
    // 创建空集合
    val emptyMap = Map.empty
    println(emptyMap)
    // 获取指定Key
    val aValue = m1.apply("a")
    println(aValue)

    println("============")
    // 获取可能存在的Key值
    val maybeInt: Option[Int] = m1.get("aa")
    if (!maybeInt.isEmpty) {
      println(maybeInt.get)
    } else {
      println(maybeInt.getOrElse(0))
    }

    println(m1.getOrElse("a", 0))


  }

}