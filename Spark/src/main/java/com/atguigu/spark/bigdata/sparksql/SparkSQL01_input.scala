package com.atguigu.spark.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author pangzl
 * @create 2022-05-05 14:44
 */
object SparkSQL01_input {

  def main(args: Array[String]): Unit = {
    // 1. 创建上下文环境配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")
    // 2. 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 3. 读取文件
    val df: DataFrame = spark.read.json("data/user.json")
    // 4. 可视化展示
    df.show()
    // 5. 关闭资源
    spark.stop()
  }
}
