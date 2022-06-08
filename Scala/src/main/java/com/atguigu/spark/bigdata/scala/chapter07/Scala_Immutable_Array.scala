package com.atguigu.spark.bigdata.scala.chapter07

/**
 * Scala 数组
 *
 * @author pangzl
 * @create 2022-04-16 9:50
 */
object Scala_Immutable_Array {

  def main(args: Array[String]): Unit = {
    // 不可变数组的语法和基本操作
    val arr = new Array[Int](4)
    println(arr.length)

    // 数据赋值
    arr(0) = 1
    arr(1) = 2
    arr(2) = 3
    arr.update(0, 8)
    arr.foreach(println(_))

    // 遍历数组
    // 根据索引查看数组
    println(arr(0))
    // 将数组中的数据生成字符串
    println(arr.mkString(", "))
    // 普通遍历 for 循环
    for (elem <- arr) {
      println(elem)
    }

    println("==============")

    // 简化遍历
    def printx(i: Int): Unit = {
      println(i)
    }

    arr.foreach(printx)
    // 匿名函数至简原则
    arr.foreach(println(_))
    arr.foreach(println)

    println("=============不可变数组基本操作============")
    // 不可变数组基本操作
    val arr1 = Array.apply(3, 4, 5, 6)
    val arr2 = Array(7, 8, 9)

    // 增加数据,会创建新的数组，并不会改变数据本身
    val arr3 = arr1.:+(1)
    val arr4: Array[Int] = arr1.+:(2)
    println(arr3.mkString(", "))
    println(arr4.mkString(", "))

    val arr5 = arr2 ++ arr1
    arr5.foreach(println)

    // 多维数组
    var myMatrix = Array.ofDim[Int](3, 3)
    myMatrix.foreach(list => println(list.mkString(",")))

    // 合并数组 arr1 arr2
    val arr6 = Array.concat(arr1, arr2)
    arr6.foreach(println)
    // 创建指定范围的数组
    val arr7 = Array.range(1, 3)
    arr7.foreach(println)

    println("==============")
    // 创建数组并填充指定数量的数组，创建完成后，把-1当成默认初始值
    val arr8 = Array.fill[Int](5)(-1)
    arr8.foreach(println)

  }

}