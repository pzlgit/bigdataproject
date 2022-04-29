package com.atguigu.bigdata.sparkcore.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-28 15:01
 */
object SparkCore_Top10_1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(conf)

    // 1. 读取数据文件，获取原始数据
    val actionDatas = sc.textFile("data/user_visit_action.txt")
    actionDatas.cache()

    // 2. 按照不同的维度（角度）对数据进行统计（WordCount）
    //    2.1 点击 => (品类ID， 点击数量)
    val clickCnt = actionDatas.filter(
      line => {
        val datas = line.split("_")
        datas(6) != "-1"
      }
    ).map(
      line => {
        val datas = line.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    //    2.2 下单 => (品类， 下单数量)
    val orderCnt = actionDatas.filter(
      line => {
        val datas = line.split("_")
        datas(8) != "null"
      }
    ).flatMap(
      line => {
        val datas = line.split("_")
        val ids = datas(8).split(",")
        ids.map(
          id => {
            (id, 1)
          }
        )
      }
    ).reduceByKey(_ + _)

    //    2.3 支付 => (品类， 支付数量)
    val payCnt = actionDatas.filter(
      line => {
        val datas = line.split("_")
        datas(10) != "null"
      }
    ).flatMap(
      line => {
        val datas = line.split("_")
        val ids = datas(10).split(",")
        ids.map(
          id => {
            (id, 1)
          }
        )
      }
    ).reduceByKey(_ + _)

    // 3. 将统计结果进行排序： 点击数量 > 下单数量 > 支付数量
    // 元组的排序：先排第一个，如果第一个相同，排第二个，依此类推
    // 将三个不同的数据集（RDD）转换成一个RDD。
    // (k, v1) cogroup (k, v2) => ( k, (v1, v2) )
    val cogroupDatas: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
    clickCnt.cogroup(orderCnt, payCnt)
    val mergeDatas: RDD[(String, (Int, Int, Int))] = cogroupDatas.mapValues {
      case (clickIter, orderIter, payIter) => {
        val clickCount = clickIter.headOption.getOrElse(0)
        val orderCount = orderIter.headOption.getOrElse(0)
        val payCount = payIter.headOption.getOrElse(0)
        (clickCount, orderCount, payCount)
      }
    }
    // 4. 将合并后的数据进行排序，取前10名
    val top10: Array[(String, (Int, Int, Int))] = mergeDatas.sortBy(_._2, false).take(10)
    top10.foreach(println)
    sc.stop()
  }
}
