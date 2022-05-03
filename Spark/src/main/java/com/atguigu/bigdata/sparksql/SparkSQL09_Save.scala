package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-03 19:57
 */
object SparkSQL09_Save {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    val df: DataFrame = spark.read.json("data/user.json")

    df.write.mode("overwrite").save("data/output")


//    val dataFrame: DataFrame = spark.read.load("data/output")
//    dataFrame.show()

    // 指定数据类型保存
    // df.write.json("data/output1")

    spark.stop()
  }
}
