package com.atguigu.spark.bigdata.scala.chapter11

/**
 * Scala 泛型转换
 *
 * @author pangzl
 * @create 2022-04-20 18:44
 */
object Scala_FanXing1 {

  def main(args: Array[String]): Unit = {
    // 泛型不可变
    val u1: Test1[User11] = new Test1[User11]
    //val u2: Test1[User11] = new Test1[Parent11]
    //val u3: Test1[User11] = new Test1[SubUser11]
  }

  class Test1[T] {}

  class Parent11 {}

  class User11 extends Parent11 {}

  class SubUser11 extends User11 {}
}