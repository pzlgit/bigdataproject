package com.atguigu.bigdata.scala.chapter08

/**
 * Scala 偏函数
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_11 {

  def main(args: Array[String]): Unit = {
    val pf: PartialFunction[Int, String] = {
      case 1 => "one"
      case 2 => "two"
    }

    println(List(1, 2, 3).collect(pf))

    //将该List(1,2,3,4,5,6,"test")中的Int类型的元素加一，并去掉字符串。
    val list = List(1, 2, 3, 4, 5, 6, "test")
    // 不适用偏函数
    list
      .filter(_.isInstanceOf[Int])
      .map(_.asInstanceOf[Int] + 1)
      .foreach(println)

    // 使用偏函数
    val list1 = list.collect {
      case i: Int => i + 1
    }
    println(list1)
  }

}