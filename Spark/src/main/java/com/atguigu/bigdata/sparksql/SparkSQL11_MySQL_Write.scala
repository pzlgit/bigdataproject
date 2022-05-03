package com.atguigu.bigdata.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-03 20:16
 */
object SparkSQL11_MySQL_Write {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()
    // 向MySQL中写数据
    // 1. 准备数据
    val rdd: RDD[User] = spark.sparkContext.makeRDD(
      List(User(3000, "zhangsan"), User(3001, "lisi"))
    )
    // 2. 将rdd转化为 DataSet
    import spark.implicits._
    val ds: Dataset[User] = rdd.toDS()
    // 3. 向mysql中写入数据
    ds.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

  case class User(id: Int, name: String)
}
