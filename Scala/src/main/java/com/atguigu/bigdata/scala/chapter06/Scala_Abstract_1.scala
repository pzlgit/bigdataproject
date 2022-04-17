package com.atguigu.bigdata.scala.chapter06

/**
 * Scala 抽象属性
 *
 * @author pangzl
 * @create 2022-04-15 20:22
 */
object Scala_Abstract_1 {

  def main(args: Array[String]): Unit = {
    val child = new Child()
  }
}

abstract class User {
  var name: String
  val age: Int = 30
}

class Child extends User {
  var name: String = "zhangsan"
  override val age: Int = 20
}