package com.atguigu.spark.bigdata.sparksql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:37
 */
object SparkSQL07_UDAF_OldVersion_Class {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    val df: DataFrame = spark.read.json("data/user.json")

    import spark.implicits._
    val ds: Dataset[User] = df.as[User]

    ds.select(new MyAvgAgeUDAFClass().toColumn).show()

    spark.stop()
  }

  case class User(name: String, age: Long)

  case class Buffer(var total: Long, var count: Long)

  class MyAvgAgeUDAFClass extends Aggregator[User, Buffer, Long] {

    override def zero: Buffer = {
      Buffer(0L, 0L)
    }

    override def reduce(buffer: Buffer, user: User): Buffer = {
      buffer.total += user.age
      buffer.count += 1
      buffer
    }

    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      b1.count += b2.count
      b1
    }

    override def finish(buffer: Buffer): Long = {
      buffer.total / buffer.count
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong

  }

}
