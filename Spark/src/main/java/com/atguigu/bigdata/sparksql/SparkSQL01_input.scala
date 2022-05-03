package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-03 18:40
 */
object SparkSQL01_input {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 读取数据
    val df: DataFrame = spark.read.json("data/user.json")
    // 展示数据
    df.show()

    spark.stop()
  }
}
