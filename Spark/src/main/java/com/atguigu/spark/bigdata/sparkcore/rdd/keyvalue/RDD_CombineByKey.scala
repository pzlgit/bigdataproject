package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:14
 */
object RDD_CombineByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 1 创建RDD
    val list: List[(String, Int)] =
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)
    // 针对一个RDD,计算每种key对应的值的和，再根据key计算每种key的均值
    val rdd3 = input.combineByKey(
      v => (v, 1),
      (acc: (Int, Int), v) => {
        (acc._1 + v, acc._2 + 1)
      },
      (acc1: (Int, Int), acc2: (Int, Int)) => {
        (acc1._1 + acc2._1, acc1._2 + acc2._2)
      }
    )
    rdd3.collect().foreach(println)

    // 求均值
    val rdd4 = rdd3.map {
      case (key, (num, cnt)) => {
        (key, num / cnt.toDouble)
      }
    }
    rdd4.collect().foreach(println)
    sc.stop()
  }
}
