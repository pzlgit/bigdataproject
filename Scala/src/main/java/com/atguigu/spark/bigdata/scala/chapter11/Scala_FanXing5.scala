package com.atguigu.spark.bigdata.scala.chapter11

/**
 * Scala 上下文限定
 *
 * @author pangzl
 * @create 2022-04-20 18:59
 */
object Scala_FanXing5 {

  def main(args: Array[String]): Unit = {
    def f[A : Test](a: A) = println(a)
    implicit val test : Test[User] = new Test[User]
    f( new User() )
  }
  class Test[T] {
  }
  class Parent {
  }
  class User extends Parent{
  }
  class SubUser extends User {
  }
}