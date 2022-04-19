package com.atguigu.bigdata.scala.chapter08

/**
 * Scala 模式匹配
 *
 * @author pangzl
 * @create 2022-04-19 18:08
 */
object Scala_Match_6 {

  def main(args: Array[String]): Unit = {
    // Scala 匹配对象
    val user = User12("zhangsan", 10)
    val result = user match {
      case User12("zhangsan", 11) => "yes"
      case _ => "no"
    }
    println(result)

  }

}

class User12(val name: String, val age: Int)

object User12 {
  def apply(name: String, age: Int): User12 = new User12(name, age)

  def unapply(user: User12): Option[(String, Int)] = {
    if (user == null) None
    else Some(user.name, user.age)
  }
}