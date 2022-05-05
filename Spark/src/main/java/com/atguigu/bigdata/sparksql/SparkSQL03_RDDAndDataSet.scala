package com.atguigu.bigdata.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-05 15:00
 */
object SparkSQL03_RDDAndDataSet {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rdd: RDD[String] = sc.textFile("data/user.txt")

    rdd.map(
      line =>{
        val words: Array[String] = line.split(",")
         User(words(0),words(1).toLong)
      }
    ).toDS().show()

    rdd.map(
      line =>{
        val words: Array[String] = line.split(",")
        User(words(0),words(1).toLong)
      }
    ).toDS().rdd.collect().foreach(println)

    sc.stop()
  }

  case class User(name : String,age :Long)
}
