package com.atguigu.bigdata.sparkcore.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 闭包检测 案例
 *
 * @author pangzl
 * @create 2022-05-08 10:37
 */
object serializable01_object {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // 创建两个User
    val user1 = new User()
    user1.name = "zhangsan"
    val user2 = new User()
    user2.name = "lisi"

    // 创建RDD
    val userRDD: RDD[User] = sc.makeRDD(List(user1, user2))

    // foreach 打印 报错：java.io.NotSerializableException
    userRDD.foreach(user => println(user.name))

    // 打印 RIGHT: 因为没有传对象到Executor端
    val userRDD2: RDD[User] = sc.makeRDD(List())
    userRDD2.foreach(user => println(user.name))

    // 打印 Error:Task not serializable 没执行就报错了
    userRDD2.foreach(user => println(user1.name))

    sc.stop()
  }

  // 自定义类需要序列化
  class User() extends Serializable {
    var name: String = _
  }
}
