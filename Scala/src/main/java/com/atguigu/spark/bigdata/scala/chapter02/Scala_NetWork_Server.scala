package com.atguigu.spark.bigdata.scala.chapter02

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ServerSocket, Socket}

/**
 * Scala 网络
 */
object Scala_NetWork_Server {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9999)
    while (true) {
      // 等待客户端连接Server
      val socket: Socket = server.accept()
      val reader = new BufferedReader(
        new InputStreamReader(
          socket.getInputStream,
          "UTF-8"
        )
      )
      var s: String = ""
      var flg = true
      while (flg) {
        s = reader.readLine()
        if (s != null) {
          println(s)
        } else {
          flg = false
        }
      }
    }
  }

}