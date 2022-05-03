package com.atguigu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * 自定义UDAF聚合函数实现求平均年龄
 *
 * @author pangzl
 * @create 2022-05-03 19:17
 */
object SparkSQL06_UDAF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    // 1. 读取文件数据
    val df: DataFrame = spark.read.json("data/user.json")
    // 2. 创建临时视图
    df.createOrReplaceTempView("user")
    // 注册UDAF函数
    spark.udf.register("MyAvg", functions.udaf(new MyAvgUDAF()))
    // 编写SQL
    spark.sql("select MyAvg(age) from user").show()
    spark.stop()
  }

  case class Buff(var sum: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {

    // 初始化缓冲区
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 将输入的年龄和缓冲区中的数据聚合
    override def reduce(buff: Buff, age: Long): Buff = {
      buff.sum += age
      buff.count += 1
      buff
    }

    // 多个缓冲区数据合并
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

    // 完成聚合操作，输出最终结果
    override def finish(reduction: Buff): Long = {
      reduction.sum / reduction.count
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong

  }

}
