package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从MySQL中读取数据
 *
 * @author pangzl
 * @create 2022-05-05 16:07
 */
object SparkSQL10_MySQL_Read {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    // 通用的load方法读取jdbc连接
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()

    df.createOrReplaceTempView("user")

    spark.sql("select id,name from user").show()

    spark.stop()
  }
}
