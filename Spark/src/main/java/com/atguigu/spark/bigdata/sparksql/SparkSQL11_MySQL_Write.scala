package com.atguigu.spark.bigdata.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * 向 MySQL中写数据
 *
 * @author pangzl
 * @create 2022-05-05 16:10
 */
object SparkSQL11_MySQL_Write {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .getOrCreate()

    import spark.implicits._

    // 准备数据
    val rdd: RDD[User] = spark.sparkContext.makeRDD(List(User(3002, "zhangsan"), User(3003, "lisi")))
    val ds: Dataset[User] = rdd.toDS()
    // 向Mysql中写入数据
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
