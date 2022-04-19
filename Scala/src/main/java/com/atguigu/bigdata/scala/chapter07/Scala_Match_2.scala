package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 9:53
 */
object Scala_Match_2 {

  def main(args: Array[String]): Unit = {
    // Scala 模式变量声明
    // 元组声明
    val (x,y) = (1, 2)
    println(s"x=$x,y=$y")
    // Array声明
    val Array(first,second,_*) = Array(1, 7, 2, 9)
    println(s"first=$first,second=$second")
    // 对象声明
    val Person11(name,age) = Person11("zhangsan", 16)
    println(s"name=$name,age=$age")
  }


}

case class Person11(name : String,age : Int)