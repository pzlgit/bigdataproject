package com.atguigu.bigdata.scala.chapter06

/**
 * Scala 抽象方法
 *
 * @author pangzl
 * @create 2022-04-15 20:30
 */
object Scala_Abstract_2 {

  def main(args: Array[String]): Unit = {
    val user = new ChildUser()
    user.test()
  }

}

abstract class User6 {
  def test(): Unit = {
    println("User6 test ...")
  }
}
class ChildUser extends User6 {
  override def test(): Unit = {
    println("ChildUser test ...")
  }
}