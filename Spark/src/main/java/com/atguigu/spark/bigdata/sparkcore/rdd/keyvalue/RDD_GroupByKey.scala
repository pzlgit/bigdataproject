package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey 按照k重新分组
 *
 * @author pangzl
 * @create 2022-04-27 18:54
 */
object RDD_GroupByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 统计单词出现次数
    val rdd = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
    // 将相同key分组
    val group: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    //    rdd2.collect().foreach(println)
    // 计算value求和
    group.map {
      case (word, iterator) => {
        (word, iterator.sum)
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
