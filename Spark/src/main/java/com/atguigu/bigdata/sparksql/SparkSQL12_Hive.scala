package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.SparkSession

/**
 *
 * @author pangzl
 * @create 2022-05-03 20:36
 */
object SparkSQL12_Hive {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .enableHiveSupport()
      .getOrCreate()

    // 连接外部Hive并进行操作
    spark.sql("show tables").show()

    spark.stop()
  }
}
