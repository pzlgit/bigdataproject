package com.atguigu.spark.bigdata.scala.chapter06

/**
 * Scala 构造方法
 *
 * @author pangzl
 * @create 2022-04-15 19:27
 */
object Scala_Construct {

  def main(args: Array[String]): Unit = {
    val user = new User1("11")
    println(user.userName)

  }

}

class User1(name: String) { // 主构造函数，完成类的初始化

  var userName: String = name

  //  def this(userName: String) { // 辅助构造函数，使用this声明
  //    this(userName) // 辅助构造函数必须直接或者间接调用主构造函数
  //    this.userName = name
  //  }

  def this(userName: String, password: String) {
    this(userName) // 调用其他构造器，要求被调用构造器必须提前声明
  }

}