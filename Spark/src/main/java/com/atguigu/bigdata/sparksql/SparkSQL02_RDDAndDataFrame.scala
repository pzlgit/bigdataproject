package com.atguigu.bigdata.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-05 14:48
 */
object SparkSQL02_RDDAndDataFrame {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/user.txt")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 手动转换为DataFrame
    import spark.implicits._
    val df: DataFrame = rdd.map(
      line => {
        val words: Array[String] = line.split(",")
        (words(0), words(1).toInt)
      }
    ).toDF("name", "age")
    df.show()

    // 通过样例类转换为DataFrame
    rdd.map(
      line => {
        val words: Array[String] = line.split(",")
        User(words(0),words(1).toInt)
      }
    ).toDF().show()

    // DataFrame 转换为 RDD
    rdd.map(
      line => {
        val words: Array[String] = line.split(",")
        User(words(0),words(1).toInt)
      }
    ).toDF().rdd.collect().foreach(println)

    sc.stop()
  }

  case class User(name: String, age: Int)

}
