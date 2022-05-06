package com.atguigu.bigdata.sparkcore.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 闭包检测
 *
 * @author pangzl
 * @create 2022-05-06 20:55
 */
object serializable01_object {

  def main(args: Array[String]): Unit = {
   val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
   val sc: SparkContext = new SparkContext(conf)

    // 构建RDD
    val user1 = new User
    val user2 = new User
    user1.name = "zhangsan"
    user2.name = "lisi"

    val rdd: RDD[User] = sc.makeRDD(List(user1, user2))

    // foreach 打印 java.io.NotSerializableException
    // rdd.foreach(user => println(user.name))

    val rdd2: RDD[User] = sc.makeRDD(List())
    rdd2.foreach(user => println(user.name))

    // Error   Task not serializable 没执行就报错了
    // rdd2.foreach(user => println(user1.name))

    sc.stop()
  }

  class User {
    var name: String = _
  }

}
