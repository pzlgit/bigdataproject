package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.SparkSession

/**
 *
 * @author pangzl
 * @create 2022-05-03 19:55
 */
object SparkSQL08_Load {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    spark.read.format("json").load("data/user.json").show()

    // format 指定加载数据类型


    spark.stop()
  }
}
