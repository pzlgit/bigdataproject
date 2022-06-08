package com.atguigu.spark.bigdata.sparkcore.rdd.base

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-25 16:02
 */
object partition04_file_test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    /*
       数据文件内容：
         ctrl@@ => 012345
         +@@    => 678
         c      => 9
       分区数量选择：
         totalSize = 10 byte
         goalSize = totalSize / 3 = 3
         分区数量 = totalSize / goalSize = 3...1 = 3 + 1 = 4
       分析：
         [0,0+3] => [0,3] => ctrl
         [3,3+3] => [3,6} => +
         [6,6+3] => [6,9] => c
         [9,9+1] => [9,10] =>
     */
    sc.textFile("Data/3.txt", 3).saveAsTextFile("Data/output1")
    sc.stop()
  }
}
