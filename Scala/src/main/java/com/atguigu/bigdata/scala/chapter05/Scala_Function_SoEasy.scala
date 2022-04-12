package com.atguigu.bigdata.scala.chapter05

/**
 * Scala 函数至简原则
 */
object Scala_Function_SoEasy {

  def main(args: Array[String]): Unit = {
    /**
     * 至简原则： 能省则省
     */
    // 省略return关键字
    def fun1(): String = {
      "name"
    }

    // 省略花括号
    def fun2(): String = "name"

    // 省略返回值类型，根据返回值自动推断出类型
    def fun3() = "name"

    // 省略参数列表,参数列表省略了，那么调用时，小括号也不能写
    def fun4 = "name"

    fun4

    // 省略等号
    // 如果函数体中有明确的return 语句，那么返回值类型不能省略
    def fun5(name: String): String = {
      return "name"
    }

    println("---")

    //如果返回值类型是Unit，那么即使函数体中有return关键字也不生效
    def fun6(): Unit = {
      return "name"
    }

    println(fun6())

    def fun7() {
      println("name")
    }

    //省略函数名称和关键字def
    () => {
      println("--")
    }


  }

}