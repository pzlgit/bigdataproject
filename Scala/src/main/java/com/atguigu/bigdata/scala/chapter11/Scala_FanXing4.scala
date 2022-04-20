package com.atguigu.bigdata.scala.chapter11

/**
 * Scala 泛型上限 与 泛型上限
 *
 * @author pangzl
 * @create 2022-04-20 18:53
 */
object Scala_FanXing4 {

  def main(args: Array[String]): Unit = {
    test[User14](new User14())
    test[Parent14](new Parent14())
    //test[SubUser14](new SubUser14())
  }

  def test[A >: User14](a: A): Unit = {
    println(a)
  }

  class Parent14 {}

  class User14 extends Parent14 {}

  class SubUser14 extends User14 {}
}