package com.atguigu.spark.bigdata.sparkcore.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Kryo 序列化
 *
 * @author pangzl
 * @create 2022-05-08 10:59
 */
object serializable_Kryo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkCoreTest")
      // 替换默认的序列化机制
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Searcher]))
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world","hello atguigu"), 2)
    val searcher = new Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)

    sc.stop()
  }

  case class Searcher(val query: String) {

    def isMatch(s: String) = {
      s.contains(query)
    }

    def getMatchedRDD1(rdd: RDD[String]) = {
      rdd.filter(isMatch)
    }

    def getMatchedRDD2(rdd: RDD[String]) = {
      val q = query
      rdd.filter(_.contains(q))
    }

  }
}
