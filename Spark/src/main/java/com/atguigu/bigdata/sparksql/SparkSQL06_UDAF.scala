package com.atguigu.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:15
 */
object SparkSQL06_UDAF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    // 注册自定义UDAF
    spark.udf.register("myAvg", functions.udaf(new MyAvgUDAF))

    spark.sql("select myAvg(age) from user").show()

    spark.stop()
  }

  case class Buff(var total: Long,var count: Long)

  /**
   * 自定义UDAF
   * IN : Long
   * Buff : total: Long count: Long
   * Out : Double
   */
  class MyAvgUDAF extends Aggregator[Long, Buff, Double] {

    // 初始化缓冲区
    override def zero: Buff = Buff(0L, 0L)

    // 数据聚合
    override def reduce(buff: Buff, age: Long): Buff = {
      buff.total += age
      buff.count += 1
      buff
    }

    // 多个缓冲区数据合并
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    // 最终结果
    override def finish(buff: Buff): Double = {
      buff.total / buff.count
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  }

}
