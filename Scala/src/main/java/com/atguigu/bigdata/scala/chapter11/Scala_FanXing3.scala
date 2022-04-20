package com.atguigu.bigdata.scala.chapter11

/**
 * 泛型边界
 *
 * @author pangzl
 * @create 2022-04-20 18:51
 */
object Scala_FanXing3 {

  def main(args: Array[String]): Unit = {
    //test[User13](new Parent13())
    test[User13](new User13())
    test[User13](new SubUser13())
  }

  def test[A](a: A): Unit = {
    println(a)
  }

  class Parent13 {}

  class User13 extends Parent13 {}

  class SubUser13 extends User13 {}

}
