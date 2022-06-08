package com.atguigu.spark.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 自定义一个UDF实现name名称前添加前缀Name:
 *
 * @author pangzl
 * @create 2022-05-05 15:08
 */
object SparkSQL05_UDF {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    val df: DataFrame = spark.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    // 自定义UDF函数
    spark.udf.register("prefixName", (x: String) => {
      "Name:" + x
    })

    spark.sql("select prefixName(name),age from user").show()

    spark.stop()
  }
}
