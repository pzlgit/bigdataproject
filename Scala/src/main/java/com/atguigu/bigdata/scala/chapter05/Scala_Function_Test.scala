package com.atguigu.bigdata.scala.chapter05

/**
 * Scala 高阶函数 练习
 */
object Scala_Function_Test {

  def main(args: Array[String]): Unit = {

    /**
     * test(10,20,_+_)
     */
    def fun0(x: Int, y: Int): Int = {
      x + y
    }

    def test(x: Int, y: Int, f: (Int, Int) => Int) = {
      f(x, y)
    }

    println(test(10, 20, fun0))

    /**
     * test1(_.substring(0,1))
     */
    def fun1(name: String): String = {
      name.substring(0, 1)
    }

    def test1(f: String => String) = {
      f("zhangsan")
    }

    println(test1(fun1))

    /**
     * test2(_ * 2)
     */
    def fun2(x: Int): Int = {
      x * 2
    }

    def test2(f: Int => Int) = {
      f(10)
    }

    println(test2(fun2))

    // 自定义控制抽象语法-自定义while循环
    def myWhile(condition: => Boolean)(op: => Unit): Unit = {
      if (condition) {
        op
        myWhile(condition)(op)
      }
    }

    var a = 0
    myWhile(a < 5) {
      println(a)
      a += 1
    }

  }

}