package com.atguigu.bigdata.scala.chapter02

/**
 * Scala 变量
 */
object Scala_Variable {

  def main(args: Array[String]): Unit = {
    4
    /**
     * 变量声明规则
     *  1. 采用 var|val 关键字来声明变量
     *  2. var | val  变量名称：变量类型 = 变量值
     *  3. 变量名称和变量类型之间采用冒号分割
     *  4. 变量必须显示的初始化
     */
    var a: String = "lisi"
    val b: String = "wangwu"
    println(a)
    println(b)

    /**
     * 变量的类型如果能够通过变量值推断出来，那么可以省略类型声明，
     * 这里的省略，并不是不声明，而是由Scala编译器在编译时自动声明编译的。
     * （多态不适用此场景）
     */
    val c = "String"
    val d = 10
    println(c)
    println(d)

    /**
     * Java语法中变量在使用前进行初始化就可以，但是Scala语法中是不允许的，必须显示进行初始化操作。
     */
    // val e  ;  error
    val e: String = "niubi"
    println(e)

    /**
     * 可变变量var与不可变变量val
     */
    var name = "zhangsan"
    name = "lisi"
    println(name)

    val name1 = "zhangsan"
    // name1 = "niubai"

  }

}
