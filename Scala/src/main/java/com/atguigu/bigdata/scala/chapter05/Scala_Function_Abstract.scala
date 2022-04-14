package com.atguigu.bigdata.scala.chapter05

/**
 * Scala 控制抽象 使用
 */
object Scala_Function_Abstract {

  def main(args: Array[String]): Unit = {
    //    def fun(f: => Int) = {
    //      f
    //    }
    //
    //    def test(): Int = {
    //      println("---")
    //      1
    //    }
    //
    //    fun {
    //      println("22")
    //      22
    //    }

    // breakable 查看控制抽象
    //    Breaks.breakable {
    //      for (i <- Range(1, 5)) {
    //        if (i == 3) {
    //          Breaks.break()
    //        }
    //        println(i)
    //      }
    //    }

    // 自定义抽象控制语法-自定义while循环
    def myWhile(condition: => Boolean)(op: => Unit): Unit = {
      if (condition) {
        op
        myWhile(condition)(op)
      }
    }

    var a = 1
    myWhile(a <= 10) {
      println(a)
      a += 1
    }
  }

}
