package com.atguigu.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:54
 */
object SparkSQL09_Save {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
//      .config(new SparkConf().set("spark.sql.sources.default","json"))
      .getOrCreate()
    val df: DataFrame = spark.read.json("data/user.json")

    df.write.mode("append").json("data/output")

    spark.stop()
  }
}
