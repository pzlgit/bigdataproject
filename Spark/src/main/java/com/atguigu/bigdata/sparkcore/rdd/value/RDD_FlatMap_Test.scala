package com.atguigu.bigdata.sparkcore.rdd.value

import org.apache.spark.{SparkConf, SparkContext}

/**
 * FlatMap
 *
 * @author pangzl
 * @create 2022-04-26 19:40
 */
object RDD_FlatMap_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 小功能：将List(List(1,2),3,List(4,5)) 进行扁平化操作？
    // 如果数据集中的数据规则不一致，那么处理时应该根据情况进行不同的处理，一般采用模式匹配
    val rdd1 = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    val rdd2 = rdd1.flatMap {
      case list: List[_] => {
        list
      }
      case number: Int => {
        List(number)
      }
    }
    rdd2.collect().foreach(println)

    sc.stop()
  }
}
