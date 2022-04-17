package com.atguigu.bigdata.scala

/**
 * 包对象
 */
package object chapter06 {

  val userName: String = "zhangsan"

  def test(): Unit = {
    println("package test .." + userName)
  }

}