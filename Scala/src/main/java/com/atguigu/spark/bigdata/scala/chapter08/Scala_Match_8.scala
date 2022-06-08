package com.atguigu.spark.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_8 {

  def main(args: Array[String]): Unit = {
    // 模式匹配 变量声明
    val (name, age) = ("zhangsan", 13)
    println(name + "-" + age)
    val Array(first, second, _*) = Array(1, 7, 2, 8, 9)
    println(s"first=$first,second=$second") // first=1,second=7
    val Person11(name1,age1) = Person11("zhangsan", 12)
    println(s"name=$name1,age=$age1")  // name=zhangsan,age=16
  }

}

case class Person11(name: String, age: Int)