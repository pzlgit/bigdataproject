package com.atguigu.spark.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Distinct
 *
 * @author pangzl
 * @create 2022-04-26 20:25
 */
object RDD_Distinct {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val list = List(1, 2, 3, 1, 2, 2)
    // 底层采用val seen = new mutable.HashSet[A]()去重
    val distinct = list.distinct
    // distinct 算子
    val rdd1 = sc.makeRDD(list,3)
    //  case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    val rdd2 = rdd1.distinct(1)
    rdd2.saveAsTextFile("data/output")
    sc.stop()
  }
}
