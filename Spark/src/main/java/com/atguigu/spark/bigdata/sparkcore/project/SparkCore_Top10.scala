package com.atguigu.spark.bigdata.sparkcore.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-28 14:41
 */
object SparkCore_Top10 {

  def main(args: Array[String]): Unit = {
    // 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 从文件中读取数据
    val originalRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    originalRDD.cache()

    // 获取品类点击总数 (品类，点击总数)
    val clickRDD: RDD[(String, Int)] = originalRDD
      .filter(line => {
        val words = line.split("_")
        words(6) != "-1"
      }).map(
      line => {
        val words = line.split("_")
        (words(6), 1)
      }
    ).reduceByKey(_ + _)
    // 获取品类下单总数 (品类，下单总数)
    val orderRDD: RDD[(String, Int)] = originalRDD
      .filter(line => {
        val words = line.split("_")
        words(8) != "null"
      })
      .flatMap(
        line => {
          val words = line.split("_")
          val orders = words(8).split(",")
          orders.map((_, 1))
        }
      ).reduceByKey(_ + _)
    // 获取品类支付总数 (品类，支付总数)
    val payRDD: RDD[(String, Int)] = originalRDD
      .filter(line => {
        val words = line.split("_")
        words(10) != "null"
      })
      .flatMap(
        line => {
          val words = line.split("_")
          val pays = words(10).split(",")
          pays.map((_, 1))
        }
      ).reduceByKey(_ + _)

    // 将单独统计的结果转换数据结构为 （品类，（点击总数，下单总数，支付总数））
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickRDD.cogroup(orderRDD, payRDD)
    // 对Value进行排序
    val megreRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        val clickCount = clickIter.headOption.getOrElse(0)
        val orderCount = orderIter.headOption.getOrElse(0)
        val payCount = payIter.headOption.getOrElse(0)
        (clickCount, orderCount, payCount)
      }
    }
    // 排序取Top10
    val top10: Array[(String, (Int, Int, Int))] = megreRDD.sortBy(_._2, false).take(10)

    // 打印输出
    top10.foreach(println)

    sc.stop()
  }
}
