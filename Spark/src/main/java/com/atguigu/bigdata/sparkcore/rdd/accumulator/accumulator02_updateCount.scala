package com.atguigu.bigdata.sparkcore.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-08 15:06
 */
object accumulator02_updateCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 3.创建RDD
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    // 3.1 定义累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    val value: RDD[(String, Int)] = dataRDD.map(t => {
      // 3.2 累加器添加数据
      sum.add(1)
      t
    })

    // 3.3 调用两次行动算子，map执行两次，导致最终累加器的值翻倍
    value.foreach(println)
    value.collect()

    // 3.4 获取累加器的值
    println("a:"+sum.value)

    sc.stop()
  }
}
