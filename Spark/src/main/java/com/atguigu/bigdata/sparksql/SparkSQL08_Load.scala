package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.SparkSession

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:51
 */
object SparkSQL08_Load {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    spark.read.load("data/output/part-00000-2bc8235c-a9e5-4ecc-8b07-24d3613df434-c000.snappy.parquet").show()

    spark.stop()
  }
}
