package com.atguigu.spark.bigdata.scala.chapter06

/**
 * Scala 面向对象 继承
 *
 * @author pangzl
 * @create 2022-04-15 19:58
 */
object Scala_Extends {

  def main(args: Array[String]): Unit = {
    val user = new Student2(18)
  }

}

class Student2(age: Int) extends Person2(age) {
  println("Student2")
}
class Person2(age: Int) {
  println("Person2")
}