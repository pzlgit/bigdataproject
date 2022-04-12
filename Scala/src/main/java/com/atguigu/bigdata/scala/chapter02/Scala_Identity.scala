package com.atguigu.bigdata.scala.chapter02

/**
 * Scala 标识符
 */
object Scala_Identity {

  def main(args: Array[String]): Unit = {
    /**
     * 与Java一样的标识符规则
     */
    var name = "zhangsan"
    var name2 = "zhangsan2"
    // var 3name = "zhangsan3" 数字不能开头
    val name$ = "zhangsan4"

    /**
     * 与Java不一样的标识符规则
     */
    val + = "heihei";
    println(+)

    /**
     * 特殊符合作为标识符
     */
    val `private` = "sss"
    println(`private`)

  }

}
