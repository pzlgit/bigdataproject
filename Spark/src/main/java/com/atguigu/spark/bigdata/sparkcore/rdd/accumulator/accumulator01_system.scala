package com.atguigu.spark.bigdata.sparkcore.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 系统自定义累加器 使用
 *
 * @author pangzl
 * @create 2022-05-08 14:58
 */
object accumulator01_system {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    // 传统的RDD求WordCount ，中间存在shuffle，性能比较低
    dataRDD.reduceByKey(_ + _).collect().foreach(println(_))

    // 如果不用Shuffle，怎么处理呢？ 累加器
    var sum: Long = 0L
    dataRDD.foreach {
      case (word, count) => {
        sum = sum + count
        println("sum=" + sum)
      }
    }
    println(sum)

    println("++++++++++++++++++")

    // 累加器实现
    val sumA: LongAccumulator = sc.longAccumulator("sum")

    dataRDD.foreach{
      case(word,cnt) =>{
        sumA.add(cnt)
        println(sumA.value)
      }
    }
    println(sumA.value)

    sc.stop()
  }
}
