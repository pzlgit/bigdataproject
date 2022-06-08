package com.atguigu.spark.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:05
 */
object SparkSQL04_DataFrameAndDataSet {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    val df: DataFrame = spark.read.json("data/user.json")
    import spark.implicits._

    // DF => DS
    val ds: Dataset[User] = df.as[User]
    ds.show()

    // DS => DF
    ds.toDF().show()


    spark.stop()
  }

  case class User(name: String, age: Long)
}
