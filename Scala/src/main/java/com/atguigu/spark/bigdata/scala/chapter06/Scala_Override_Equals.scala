package com.atguigu.spark.bigdata.scala.chapter06

/**
 * 重写equals方法
 *
 * @author pangzl
 * @create 2022-04-15 21:05
 */
object Scala_Override_Equals {

  def main(args: Array[String]): Unit = {
    val user1 = new User8()
    user1.id =1001
    val user2 = new User8()
    user2.id = 1001
    println(user1 == user2)
  }
}

class User8 {
  var id: Int = _
  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[User8]) {
      val u = obj.asInstanceOf[User8]
      this.id == u.id
    } else {
      false
    }
  }
}