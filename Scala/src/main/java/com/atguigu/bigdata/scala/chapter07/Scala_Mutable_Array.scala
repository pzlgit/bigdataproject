package com.atguigu.bigdata.scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * Scala 可变数组
 *
 * @author pangzl
 * @create 2022-04-17 8:42
 */
object Scala_Mutable_Array {

  def main(args: Array[String]): Unit = {
    // Scala 可变数组
    val arr = new ArrayBuffer[Int](5)
    val arr1 = ArrayBuffer.apply(1, 2, 3, 4)
    val arr2 = ArrayBuffer(5, 6, 7, 8)

    // 增加数据
    arr.append(1)
    arr.appendAll(arr2)
    arr.insert(2, 0)
    println(arr.mkString(", "))

    // 修改数据
    arr1.update(0, 0)
    val arr3 = arr1.updated(0, 1)
    println(arr1.mkString(","))
    arr3(0) = 10
    println(arr3.mkString(","))

    // 删除数据
    arr3.remove(0)
    arr3.remove(0, 2)
    println(arr3.mkString(","))

    // 查询数据
    println(arr1)
    for (elem <- arr1) {
      println(elem)
    }

    // 可变数组基本操作
    val arr5 = ArrayBuffer.apply(1, 2, 3, 4)
    val arr6 = ArrayBuffer(5, 6, 7, 8)

    //    val arr7 = arr5 += 6
    //    println(arr7)
    //    println(arr5 eq arr7)
    //    val arr7 = arr5 ++ arr6
    //    println(arr7)
    //    println(arr7 eq arr5)
    var arr7 = arr5 ++= arr6
    print(arr5)

  }

}