package com.atguigu.bigdata.sparkcore.rdd.dependence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-08 11:14
 */
object Stage01 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2), 2)
    // 聚合
    val result: RDD[(Int, Int)] = rdd.map((_, 1)).reduceByKey(_ + _)
    // job0 打印到 控制台
    result.collect().foreach(println(_))

    // job1 输出到磁盘文件
    result.saveAsTextFile("data/output")

    Thread.sleep(1000000)  // 不停,为了看4040

    sc.stop()
  }
}
