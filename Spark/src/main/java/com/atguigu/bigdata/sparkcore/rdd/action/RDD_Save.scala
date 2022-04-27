package com.atguigu.bigdata.sparkcore.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 20:53
 */
object RDD_Save {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // save 相关操作
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    // save
    rdd.saveAsTextFile("data/output1")
    rdd.saveAsObjectFile("data/output2")
    rdd.saveAsSequenceFile("data/output3")

    // read
    val rdd1: RDD[String] = sc.textFile("data/output1")
    val rdd2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("data/output2")
    val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("data/output3")
    println(rdd1.collect().mkString(","))
    println(rdd2.collect().mkString(","))
    println(rdd3.collect().mkString(","))

    sc.stop()
  }
}
