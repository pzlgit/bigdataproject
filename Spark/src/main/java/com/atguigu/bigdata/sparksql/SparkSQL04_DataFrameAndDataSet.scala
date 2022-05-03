package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-03 19:06
 */
object SparkSQL04_DataFrameAndDataSet {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 读取文件
    val df: DataFrame = spark.read.json("data/user.json")
    // DataFrame 转化为 DataSet
    import spark.implicits._
    val ds: Dataset[User] = df.as[User]
    ds.show()

    // DataSet => DataFrame
    val df1: DataFrame = ds.toDF()
    df1.show()
    spark.stop()
  }

  case class User(name :String,age : Long)
}
