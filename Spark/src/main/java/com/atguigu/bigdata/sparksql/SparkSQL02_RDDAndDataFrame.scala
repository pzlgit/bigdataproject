package com.atguigu.bigdata.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-03 18:49
 */
object SparkSQL02_RDDAndDataFrame {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 读取文件
    val lineRDD: RDD[String] = sc.textFile("data/user.txt")
    // 将文件按照逗号切分并组装数据
    val wordRDD: RDD[(String, Long)] = lineRDD.map(
      line => {
        val words: Array[String] = line.split(",")
        (words(0), words(1).trim.toInt)
      }
    )
    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    // 将RDD 转化为 DataFrame 手动转换
    import spark.implicits._
    wordRDD.toDF("name", "age").show()

    // RDD转换为DataFrame 通过样例类转换
    val df: DataFrame = wordRDD.map {
      case (name, age) => {
        User(name, age)
      }
    }.toDF()
    df.show()

    // FataFrame转化为 rdd
    val rdd: RDD[Row] = df.rdd
    rdd.collect().foreach(println(_))


    sc.stop()
  }

  case class User(name: String, age: Long)

}
