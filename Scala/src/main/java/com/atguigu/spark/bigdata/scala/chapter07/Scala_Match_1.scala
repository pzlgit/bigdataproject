package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 9:53
 */
object Scala_Match_1 {

  def main(args: Array[String]): Unit = {
    // 匹配常量
    //    def describe(x: Any) = x match {
    //      case 5 => "Int five"
    //      case "hello" => "String hello"
    //      case true => "Boolean true"
    //      case '+' => "Char +"
    //    }
    //
    //    val result = describe(5) // Int five
    //    println(result)

    // 匹配类型
    //    def describe(x: Any) = x match {
    //      case i: Int => "Int" // 类型匹配： 变量名 ：Int
    //      case s: String => "String hello"
    //      case l: List[_] => "List" // 泛型中下划线表示任意类型
    //      case arr: Array[Int] => "Array[Int]"
    //      case someThing => "something else " + someThing //someThing 是给下划线起的名
    //    }
    //
    //    val result = describe(5) // Int
    //    println(result)

    // 匹配数组
    //    for (arr <-
    //           Array(Array(0),
    //             Array(1, 0),
    //             Array(0, 1, 0),
    //             Array(1, 1, 0),
    //             Array(1, 1, 0, 1),
    //             Array("hello", 90))) { // 对一个数组集合进行遍历
    //      val result = arr match {
    //        case Array(0) => "0" // 匹配Array(0) 这个数组
    //        case Array(x, y) => x + "," + y // 匹配有两个元素的数组，将元素值赋给对应的x,y
    //        case Array(0, _*) => "以0开头的数组" // 匹配以0开头和数组
    //        case _ => "something else"
    //      }
    //      println("result = " + result)

    // 匹配列表
    //    for (list <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0), List(88))) {
    //      val result = list match {
    //        case List(0) => "0" // 匹配List(0)
    //        case List(x, y) => x + "," + y // 匹配有两个元素的List
    //        case List(0, _*) => "0 ..." // 匹配以0开头的集合
    //        case _ => "something else"
    //      }
    //      println(result)
    //    }
    //
    //    val list: List[Int] = List(1, 2, 5, 6, 7)
    //    list match {
    //      case first :: second :: rest => println(first + "-" + second + "-" + rest)
    //      case _ => println("something else")
    //    }

    // 匹配元组
    for (tuple <- Array((0, 1), (1, 0), (1, 1), (1, 0, 2))) {
      val result = tuple match {
        case (0, _) => "0 ..." // 第一个元素是0的元组
        case (y, 0) => "" + y + "0" // 匹配后一个元素是0的对偶元组
        case (a, b) => "" + a + " " + b // 两个元素的元组
        case _ => "something else"
      }
      println(result)
    }
  }


}