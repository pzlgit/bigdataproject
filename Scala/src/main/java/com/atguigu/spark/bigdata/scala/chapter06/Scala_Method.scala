package com.atguigu.spark.bigdata.scala.chapter06

/**
 * Scala Method
 *
 * @author pangzl
 * @create 2022-04-15 19:24
 */
object Scala_Method {

  def main(args: Array[String]): Unit = {
    val person = new Person()
    person.eat("fruit")
  }

}

class Person {
  def eat(food: String): Unit = {
    println("eat ..." + food)
  }
}