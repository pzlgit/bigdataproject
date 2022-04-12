package com.atguigu.bigdata.scala.chapter04

import scala.util.control.Breaks._

/**
 * Scala While 循环
 */
object Scala_While {

  def main(args: Array[String]): Unit = {
    /**
     * While 使用：先判断在执行
     */
    var i: Int = 0
    while (i < 5) {
      println(i)
      i += 1
    }
    println("---------")

    /**
     * do while 使用：先执行一次，再判断，后续再执行
     */
    var j = 5
    do {
      println(j)
    } while (j < 5)

    println("---------")

    /**
     * Scala 循环中断
     */
    breakable {
      for (i <- Range(1, 5)) {
        if (i == 3) {
          break()
        }
        println(i)
      }
    }


  }

}