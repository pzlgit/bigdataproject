package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:27
 */
object SparkSQL07_UDAF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    val ds: DataFrame = spark.read.json("data/user.json")
    ds.createOrReplaceTempView("user")

    spark.udf.register("myAvg", new MyAvgUDAF())

    spark.sql("select myAvg(age) from user").show()

    spark.stop()
  }

  class MyAvgUDAF extends UserDefinedAggregateFunction {
    // 输入参数的数据类型
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区数据类型
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("sum", LongType),
          StructField("total", LongType)
        )
      )
    }

    // 输出结果数据类型
    override def dataType: DataType = DoubleType

    // 计算稳定性
    override def deterministic: Boolean = true

    // 缓冲区数据初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 更新缓冲区中的数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    // 合并多个缓冲区数据
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
      buffer1
    }

    // 最终结果计算
    override def evaluate(buffer: Row): Double = {
      (buffer.getLong(0) / buffer.getLong(1)).toDouble
    }

  }

}
