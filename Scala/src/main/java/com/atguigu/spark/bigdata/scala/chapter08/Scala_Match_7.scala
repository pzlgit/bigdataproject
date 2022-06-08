package com.atguigu.spark.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_7 {

  def main(args: Array[String]): Unit = {
    val user= User13("zhangsan", 15)
    val result = user match {
      case User13("zhangsan", 15) => "yes"
      case _ => "no"
    }
    println(result)


  }

}

case class User13(name: String, age: Int)