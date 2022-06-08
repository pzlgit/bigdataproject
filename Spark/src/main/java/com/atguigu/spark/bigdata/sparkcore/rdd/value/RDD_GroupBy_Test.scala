package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy
 *
 * @author pangzl
 * @create 2022-04-26 19:54
 */
object RDD_GroupBy_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // wordCount
    val rdd1 = sc.makeRDD(List("Hello Hadoop", "Hi Hive", "Hello Spark", "No DataX", "Yes Kafka", "Flume", "Zookeeper"))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_, 1))
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupBy(_._1)
    val rdd5 = rdd4.map {
      case (word, iter) => {
        (word, iter.size)
      }
    }
    rdd5.collect().foreach(println(_))
    sc.stop()
  }
}
