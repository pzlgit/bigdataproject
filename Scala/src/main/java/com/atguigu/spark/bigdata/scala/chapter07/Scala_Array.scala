package com.atguigu.spark.bigdata.scala.chapter07

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Scala 数组操作
 *
 * @author pangzl
 * @create 2022-04-17 9:01
 */
object Scala_Array {

  def main(args: Array[String]): Unit = {
    // 可变数组与不可变数组之间的转换
    val arr1 = Array(1, 2, 3, 4)
    val arr2 = ArrayBuffer(5, 6, 7, 8)

    // 可变数组转化为不可变数组
    val arr3: Array[Int] = arr2.toArray
    // 不可变数组转化为可变数组
    val arr4: mutable.Buffer[Int] = arr1.toBuffer

  }

}