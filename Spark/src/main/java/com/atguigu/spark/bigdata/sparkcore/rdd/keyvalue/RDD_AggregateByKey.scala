package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:02
 */
object RDD_AggregateByKey {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 按照K处理分区内和分区间的逻辑
    // 取出每个分区内相同key的最大值然后分区间相同key的数据相加求和。
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    val rdd2 = rdd.aggregateByKey(5)(
      (x, y) => {
        math.max(x, y)
      },
      (x, y) => {
        x + y
      }
    )
    rdd2.collect().foreach(println)


    sc.stop()
  }
}
