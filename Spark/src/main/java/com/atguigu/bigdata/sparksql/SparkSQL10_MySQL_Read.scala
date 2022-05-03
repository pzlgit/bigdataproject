package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-03 20:16
 */
object SparkSQL10_MySQL_Read {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 从mysql中读取数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()
    // 创建视图
    df.createOrReplaceTempView("user")
    // 查询数据
    spark.sql("select id,name from user").show()

    spark.stop()
  }
}
