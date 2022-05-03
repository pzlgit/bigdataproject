package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 自定义聚合函数实现 弱类型
 *
 * @author pangzl
 * @create 2022-05-03 19:27
 */
object SparkSQL07_UDAF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 1. 读取文件
    val df: DataFrame = spark.read.json("data/user.json")
    // 2. 创建DataFrame临时视图
    df.createOrReplaceTempView("user")
    // 3. 注册函数UDAF
    spark.udf.register("myAvgUDAF",new MyAvgUDAF())
    // 4. 调用自定义聚合函数
    spark.sql("select myAvgUDAF(age) from user").show
    spark.stop()
  }

  // 自定义聚合函数
  class MyAvgUDAF extends UserDefinedAggregateFunction {
    // 聚合函数输入参数的数据类型
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("age", LongType)
      ))
    }

    // 聚合函数缓冲区中值的数据类型
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("sum", LongType),
        StructField("count", LongType)
      ))
    }

    // 聚合函数返回值的数据类型
    override def dataType: DataType = LongType

    // 计算稳定性
    override def deterministic: Boolean = true

    // 聚合函数缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 聚合函数更新缓冲区
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 合并缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }

  }

}
