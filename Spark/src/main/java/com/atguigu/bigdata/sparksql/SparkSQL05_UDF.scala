package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 自定义UDF函数，给name加前缀
 *
 * @author pangzl
 * @create 2022-05-03 19:13
 */
object SparkSQL05_UDF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 读取文件
    val df: DataFrame = spark.read.json("data/user.json")
    // 创建临时表
    df.createOrReplaceTempView("user")
    // 自定义UDF函数
    spark.udf.register("prefixName", (name: String) => {
      "Name:" + name
    })
    // 调用sql
    spark.sql("select prefixName(name) from user").show()
    spark.stop()
  }
}
