package com.atguigu.bigdata.scala.chapter02

import scala.collection.Iterator
import scala.io.{BufferedSource, Source, StdIn}

/**
 * Scala 输入输出
 */
object Scala_IO_Input {

  def main(args: Array[String]): Unit = {
    // 从屏幕控制台输入
    val readMessage: Int = StdIn.readInt()
    println(readMessage)

    // 从文件中获取
    val source: BufferedSource = Source.fromFile("D:\\WorkShop\\BIgDataProject\\Scala\\src\\main\\resources\\user.txt")
    val iterator: Iterator[String] = source.getLines()
    while (iterator.hasNext) {
      val line = iterator.next()
      println(line)
    }

  }

}