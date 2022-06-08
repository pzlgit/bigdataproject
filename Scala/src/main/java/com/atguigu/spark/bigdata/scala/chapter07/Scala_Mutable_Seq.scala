package com.atguigu.spark.bigdata.scala.chapter07

import scala.collection.mutable.ListBuffer

/**
 * Scala 可变集Seq
 *
 * @author pangzl
 * @create 2022-04-17 9:05
 */
object Scala_Mutable_Seq {

  def main(args: Array[String]): Unit = {
    val b1 = new ListBuffer[Int]()
    b1.append(1)
    b1.append(2, 3)
    b1.update(0, 9)
    //    b1.remove(0)
    //    b1.remove(0,2)
    b1.foreach(println(_))
    println(b1)

    // 可变集合基本操作
    val buffer1 = ListBuffer(1, 2, 3, 4)
    val buffer2 = ListBuffer(5, 6, 7, 8)
    // 增加数据
    val b3 = buffer1 :+ 5
    val b4 = buffer1 += 5
    val b5 = buffer1 ++ buffer2
    val b6 = buffer1 ++= buffer2

    println( b5 eq buffer1 )
    println( b6 eq buffer1 )

    val b7 = buffer1- 1
    val b8 = buffer2 -= 5

    println(b7 eq buffer1)
    println(b8 eq buffer2)

  }
}