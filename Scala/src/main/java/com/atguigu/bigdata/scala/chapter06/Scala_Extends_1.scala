package com.atguigu.bigdata.scala.chapter06

/**
 * Scala 继承
 *
 * @author pangzl
 * @create 2022-04-15 20:09
 */
object Scala_Extends_1 {

  def main(args: Array[String]): Unit = {
    // val person = new Person3(10) 无法创建对象
    val person = Person3.apply(18) // 采用apply创建对象
    val person1 = Person3(20) // Scala会动态识别apply,apply可以被省略
    println(person.userAge) // 18
    println(person1.userAge) //20
  }
}

class Person3 private(age: Int) {
  var userAge: Int = age
  println("Person3")
}

object Person3 {
  def apply(age: Int): Person3 = new Person3(age)
}