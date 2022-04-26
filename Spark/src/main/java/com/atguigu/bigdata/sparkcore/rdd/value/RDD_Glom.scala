package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * glom
 *
 * @author pangzl
 * @create 2022-04-26 19:49
 */
object RDD_Glom {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // glom 分区转换数组
    // 创建一个2个分区的RDD，并将每个分区的数据放到一个数组，求出每个分区的最大值和所有分区最大值的和。
    val rdd1 = sc.makeRDD(1 to 4, 2)
    val rdd2: RDD[Array[Int]] = rdd1.glom()
    val rdd3: RDD[Int] = rdd2.map(_.max)
    rdd3.collect().foreach(println(_))
    val sum: Int = rdd3.collect().sum
    println(sum)
    sc.stop()
  }
}
