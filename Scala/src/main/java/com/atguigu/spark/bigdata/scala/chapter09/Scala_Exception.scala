package com.atguigu.spark.bigdata.scala.chapter09

/**
 * Scala Exception 异常处理
 *
 * @author pangzl
 * @create 2022-04-19 18:51
 */
object Scala_Exception {

  def main(args: Array[String]): Unit = {
    try {
      val i: Int = 0
      val j: Int = 10 / i
      println(j)
    } catch {
      case e: ArithmeticException => {
        println("ArithmeticException")
        println(e.getMessage)
      }
      case e: Exception => {
        println("Exception")
        println(e.getMessage)
      }
    } finally {
      println("finally")
    }

    @throws[Exception]
    def test(): Nothing = {
      throw new Exception()
    }

  }
}