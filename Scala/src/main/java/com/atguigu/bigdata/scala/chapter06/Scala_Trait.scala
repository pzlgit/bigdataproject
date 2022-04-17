package com.atguigu.bigdata.scala.chapter06

/**
 * Scala 特质 Trait
 *
 * @author pangzl
 * @create 2022-04-15 20:41
 */
object Scala_Trait {

  def main(args: Array[String]): Unit = {
    val cat = new Cat()
    cat.run()
    val user = new User7()
    user.run()
  }

}

trait Run {
  def run(): Unit
}
class Cat extends Run {
  override def run(): Unit = {
    println("Cat run...")
  }
}
class Person7 {
}
class User7 extends Person7 with Run {
  override def run(): Unit = {
    println("User run...")
  }
}