package com.atguigu.bigdata.scala.chapter02

/**
 * Scala String 字符串
 */
object Scala_String {

  def main(args: Array[String]): Unit = {
    /**
     * 字符串声明
     */
    val name: String = "zhangsan"
    println(name)

    /**
     * 字符串操作
     */
    println("hello " + name)
    println(name.substring(0, 1))

    /**
     * 字符串格式化
     */
    printf("name=%s\n", name)

    /**
     * 插值字符串
     */
    println(s"name1=${name}")

    /**
     * 多行字符串
     */
    println(
      s"""
         - hello
         - ${name}
         -""".stripMargin('-')
    )

  }

}