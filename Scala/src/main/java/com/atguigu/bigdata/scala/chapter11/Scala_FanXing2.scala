package com.atguigu.bigdata.scala.chapter11

/**
 * Scala 泛型转换
 *
 * @author pangzl
 * @create 2022-04-20 18:47
 */
object Scala_FanXing2 {

  def main(args: Array[String]): Unit = {
    // 泛型协变 与 泛型逆变
    val u1: Test12[User12] = new Test12[User12]
    val u2: Test12[User12] = new Test12[Parent12]
    //val u3: Test12[User12] = new Test12[SubUser12]
  }

  class Test12[-T] {}

  class Parent12 {}

  class User12 extends Parent12 {}

  class SubUser12 extends User12 {}
}
