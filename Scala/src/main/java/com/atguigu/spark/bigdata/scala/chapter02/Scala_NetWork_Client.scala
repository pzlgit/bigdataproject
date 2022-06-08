package com.atguigu.spark.bigdata.scala.chapter02

import java.io.{OutputStreamWriter, PrintWriter}
import java.net.Socket

/**
 * Scala 网络
 */
object Scala_NetWork_Client {

  def main(args: Array[String]): Unit = {
    val client = new Socket("localhost", 9999)
    val out = new PrintWriter(
      new OutputStreamWriter(
        client.getOutputStream,
        "UTF-8"
      )
    )
    out.print("hello Scala")
    out.flush()
    out.close()
    client.close()
  }

}