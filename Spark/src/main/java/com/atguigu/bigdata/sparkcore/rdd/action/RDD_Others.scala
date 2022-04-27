package com.atguigu.bigdata.sparkcore.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 20:40
 */
object RDD_Others {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 5, 2, 3, 4))
    // count 返回RDD中元素个数
    println(rdd.count())
    // first 返回RDD中的第一个元素
    println(rdd.first())
    // take 返回由RDD的前n个元素组成的数组
    println(rdd.take(2).mkString("Array(", ", ", ")"))
    //  takeOrdered返回该RDD排序后前n个元素组成的数组
    println(rdd.takeOrdered(2).mkString(","))

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)
    // 将该RDD所有元素相加得到结果
    val result1 = rdd1.aggregate(0)(_ + _, _ + _)
    val result2 = rdd1.aggregate(10)(_ + _, _ + _)
    println(result1)
    println(result2)

    val result3 = rdd1.fold(0)(_ + _)
    val result4 = rdd1.fold(10)(_ + _)
    println(result3)
    println(result4)

    // countByKey 统计每种key的个数
    val rdd4: RDD[(Int, String)] =
      sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val result: collection.Map[Int, Long] = rdd4.countByKey()
   println(result)
    sc.stop()
  }
}
