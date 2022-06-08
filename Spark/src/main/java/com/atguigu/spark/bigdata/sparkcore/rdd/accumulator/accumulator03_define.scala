package com.atguigu.spark.bigdata.sparkcore.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 * 自定义累加器
 *
 * @author pangzl
 * @create 2022-05-08 15:08
 */
object accumulator03_define {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 3. 创建RDD
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Spark", "Spark"), 2)

    // 创建累加器
    val accumulator = new MyAccumulator
    sc.register(accumulator,"wordCount")

    rdd.foreach(
      word =>{
        accumulator.add(word)
      }
    )

    println(accumulator.value)


    sc.stop()
  }

  // 自定义累加器
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    // 定义输出数据集合
    var map: mutable.Map[String, Long] = mutable.Map[String, Long]()

    // 是否为初始化状态，如果集合数据为空，即为初始化状态
    override def isZero: Boolean = map.isEmpty

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 重置累加器
    override def reset(): Unit = map.clear()

    // 增加数据
    override def add(v: String): Unit = {
      if (v.startsWith("H")) {
        map(v) = map.getOrElse(v, 0L) + 1
      }
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      other.value.foreach {
        case (word, cnt) => {
          map(word) = map.getOrElse(word, 0L) + cnt
        }
      }
    }

    // 累加器的值
    override def value: mutable.Map[String, Long] = map
  }

}
