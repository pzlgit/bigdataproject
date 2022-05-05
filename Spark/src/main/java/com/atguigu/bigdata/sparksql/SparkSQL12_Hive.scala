package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.SparkSession

/**
 *
 * @author pangzl
 * @create 2022-05-05 16:19
 */
object SparkSQL12_Hive {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .enableHiveSupport()
      .getOrCreate()

    // 连接Hive并进行操作
    spark.sql("show tables").show()


    spark.stop()
  }
}
