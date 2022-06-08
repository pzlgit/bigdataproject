package com.atguigu.spark.bigdata.scala.chapter06

import java.lang.reflect.Field

/**
 * 反射修改字符串
 *
 * @author pangzl
 * @create 2022-04-15 20:57
 */
object Scala_String_Ref {

  def main(args: Array[String]): Unit = {
    val str: String = " a b "
    val strClass: Class[String] = classOf[String]
    // 读取字符串底层数组
    val field: Field = strClass.getDeclaredField("value")
    field.setAccessible(true)
    val charArr = field.get(str).asInstanceOf[Array[Char]]
    charArr.update(2, 'c')
    println("-" + str + "-")
  }

}