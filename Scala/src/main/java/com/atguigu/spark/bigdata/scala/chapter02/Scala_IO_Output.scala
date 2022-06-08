package com.atguigu.spark.bigdata.scala.chapter02

import java.io.{File, PrintWriter}

/**
 * Scala 输入输出
 */
object Scala_IO_Output {

  def main(args: Array[String]): Unit = {
    // Scala 写出文件
    val wirter = new PrintWriter(new File("D:\\WorkShop\\BIgDataProject\\Scala\\src\\main\\resources\\user1.txt"))
    wirter.write("zhangssan")
    wirter.close()
  }

}