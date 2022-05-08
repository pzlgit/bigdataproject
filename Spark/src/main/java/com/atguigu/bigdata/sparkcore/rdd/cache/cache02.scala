package com.atguigu.bigdata.sparkcore.rdd.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-08 11:27
 */
object cache02 {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf并设置App名称
    val conf: SparkConf =
      new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    // 2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 3. 创建一个RDD，读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("data/word.txt" )

    // 3.1.业务逻辑
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))
    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map {
      word => {
        println("************")
        (word, 1)
      }
    }

    // 3.2采用reduceByKey，自带缓存
    val wordByKeyRDD: RDD[(String, Int)] = wordToOneRdd.reduceByKey(_ + _)

    // 3.3 触发执行逻辑
    wordByKeyRDD.collect()
    println("-----------------")
    println(wordByKeyRDD.toDebugString)

    // 3.4 再次触发执行逻辑
    wordByKeyRDD.collect()

    Thread.sleep(1000000)

    // 4.关闭连接
    sc.stop()
  }
}
