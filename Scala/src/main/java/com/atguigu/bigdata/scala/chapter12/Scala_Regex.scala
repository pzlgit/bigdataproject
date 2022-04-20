package com.atguigu.bigdata.scala.chapter12

import scala.util.matching.Regex

/**
 * Scala 正则表达式
 *
 * @author pangzl
 * @create 2022-04-20 19:00
 */
object Scala_Regex {

  def main(args: Array[String]): Unit = {
    // 构建正则表达式
    val pattern = "Scala".r
    val str: String = "Scala is Scalable Language Scala"
    // 匹配字符串 - 取第一个
    val option = pattern.findFirstIn(str)
    if (option.isEmpty) {
      println("没有找到匹配的字符串")
    } else {
      println("找到了对应的匹配字符串" + option.get)
    }
    println(pattern.findFirstIn(str))

    // 匹配字符串 - 取所有
    val iterator = pattern.findAllIn(str)
    while (iterator.hasNext) {
      println(iterator.next())
    }

    // 匹配字符串 - 首写字母大写，小写都可
    val regex = new Regex("(S|s)cala")
    println(regex.findAllIn("Scala is language"))
  }
}
