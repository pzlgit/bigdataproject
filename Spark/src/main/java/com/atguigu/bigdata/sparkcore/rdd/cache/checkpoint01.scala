package com.atguigu.bigdata.sparkcore.rdd.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-05-08 11:32
 */
object checkpoint01 {

  def main(args: Array[String]): Unit = {
    // 1.创建SparkConf并设置App名称
    val conf: SparkConf =
      new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    // 2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 需要设置检查点路径（当前路径）
    // 否则抛异常：Checkpoint directory has not been set in the SparkContext
    sc.setCheckpointDir("./ck")

    // 3. 创建一个RDD，读取指定位置文件:hello atguigu atguigu
    val lineRdd: RDD[String] = sc.textFile("data/word.txt")

    // 3.1.业务逻辑
    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
      word => {
        // 读取每一行的时间
        (word, System.currentTimeMillis())
      }
    }

    // 3.2 增加缓存，避免再重新跑一个job做checkpoint
    wordToOneRdd.cache()

    // 3.3 数据检查点：针对wordToOneRdd做检查点计算
    wordToOneRdd.checkpoint()

    // 3.4 触发执行逻辑，会立即启动一个新的job来专门的做checkpoint运算存储数据
    wordToOneRdd.collect().foreach(println)

    // 3.5 再次触发执行逻辑，会从checkpoint中读取数据
    wordToOneRdd.collect().foreach(println)
    wordToOneRdd.collect().foreach(println)

    Thread.sleep(10000000)

    // 4.关闭连接
    sc.stop()
  }
}
