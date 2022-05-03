package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 *
 * @author pangzl
 * @create 2022-05-03 19:39
 */
object SparkSQL07_UDAF_OldVersion_Class {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 读取文件
    val df: DataFrame = spark.read.json("data/user.json")
    // 创建临时视图
    df.createOrReplaceTempView("user")
    import spark.implicits._
    val dataSet: Dataset[User] = df.as[User]
    // 注册自定义函数
    val clazz = new MyAvgAgeUDAFClass()
    // 调用
    val result: Dataset[Long] = dataSet.select(clazz.toColumn)
    result.show()

    spark.stop()
  }

  case class User(var name: String, var age: Long)

  case class Buff(var total: Long, var count: Long)

  class MyAvgAgeUDAFClass extends Aggregator[User, Buff, Long] {
    // 初始化缓冲区
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 聚合
    override def reduce(b: Buff, a: User): Buff = {
      b.total = b.total + a.age
      b.count = b.count + 1
      b
    }

    // 合并
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    // 最终结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong

  }
}
