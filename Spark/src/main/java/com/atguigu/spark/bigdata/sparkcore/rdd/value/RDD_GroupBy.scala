package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupBy
 *
 * @author pangzl
 * @create 2022-04-26 19:54
 */
object RDD_GroupBy {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建一个RDD,按照元素模以2的值进行分组
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = rdd1.groupBy(_ % 2)
    rdd2.collect().foreach(println(_))
    // 创建一个RDD，按照首字母第一个单词相同分组
    val rdd3 = sc.makeRDD(List("Hadoop", "Hive", "Spark", "DataX", "Kafka", "Flume", "Zookeeper"))
    val rdd4 = rdd3.groupBy(_.substring(0, 1))
    rdd4.collect().foreach(println)
    sc.stop()
  }
}
