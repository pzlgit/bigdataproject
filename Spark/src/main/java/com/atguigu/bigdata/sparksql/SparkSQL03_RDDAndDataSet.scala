package com.atguigu.bigdata.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-03 19:01
 */
object SparkSQL03_RDDAndDataSet {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // RDD -> DataSet
    val lineRDD: RDD[String] = sc.textFile("data/user.txt")
    import spark.implicits._
    val ds: Dataset[User] = lineRDD.map(
      line => {
        val words: Array[String] = line.split(",")
        User(words(0), words(1).toLong)
      }
    ).toDS()
    ds.show()

    // DataSet => rdd
    val rdd: RDD[User] = ds.rdd
    rdd.collect().foreach(println(_))
    sc.stop()
  }

  case class User(name : String,age : Long)
}
