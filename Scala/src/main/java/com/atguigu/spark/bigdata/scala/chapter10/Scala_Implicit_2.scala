package com.atguigu.spark.bigdata.scala.chapter10

/**
 * Scala 隐式转换
 *
 * @author pangzl
 * @create 2022-04-19 18:57
 */
object Scala_Implicit_2 {

  def main(args: Array[String]): Unit = {
    // 定义隐式类
    val emp = new Emp()
    emp.insertUser()

  }

  class  Emp{}

  implicit class User20 (emp : Emp) {
    def insertUser(): Unit ={
      println("insert user")
    }
  }

}
