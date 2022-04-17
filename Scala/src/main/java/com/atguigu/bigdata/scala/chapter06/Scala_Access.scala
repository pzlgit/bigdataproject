package com.atguigu.bigdata.scala.chapter06

/**
 * Scala 访问权限
 *
 * @author pangzl
 * @create 2022-04-15 19:17
 */
object Scala_Access {

  def main(args: Array[String]): Unit = {
    // 和Java访问权限基本一致
    val car = new Car()
    println(car.age)
    println(car.speed)
  }

  class Car {
    private var name: String = "红旗"
    private[chapter06] var age: Int = 10
    protected val color: String = "red"
    val speed: Int = 100
  }

}

