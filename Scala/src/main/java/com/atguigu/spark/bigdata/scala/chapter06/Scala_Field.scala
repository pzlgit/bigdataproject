package com.atguigu.spark.bigdata.scala.chapter06

import scala.beans.BeanProperty

/**
 * Scala 属性
 *
 * @author pangzl
 * @create 2022-04-15 19:09
 */
object Scala_Field {

  def main(args: Array[String]): Unit = {
    val user = new User0()
    println(user.name)
    println(user.age)
    user.setLastName("无")
    println(user.getLastName)
  }

}

class User0 {
  val name: String = "zhangsan"
  var age = 25
  var email: String = _
  // val声明的属性不允许使用下划线赋值
  // val phone :String = _
  // @BeanProperty注解的属性不能声明在private权限的前面
  // @BeanProperty private var firstName = "zhang"
  @BeanProperty var lastName = "san"
}