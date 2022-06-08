package com.atguigu.spark.bigdata.scala.chapter07

import scala.collection.immutable
import scala.io.Source

/**
 * Scala 练习： WordCount
 *
 * @author pangzl
 * @create 2022-04-17 20:53
 */
object Scala_WordCount {

  def main(args: Array[String]): Unit = {
    // 1.从文件中获取数据源转化为List集合
    val source = Source.fromFile("D:\\WorkShop\\BIgDataProject\\Scala\\src\\main\\resources\\word.txt")
    val dataList = source.getLines().toList
    println(dataList)
    // 2.将List数据集中的每一条数据按照空格切分,扁平化为一个一个的个体
    val wordList = dataList.flatMap(
      (line: String) => {
        line.split(" ")
      }
    )
    println(wordList)
    // 3.集合映射，将集合中的元素通过映射成为元组类型
    val tupleList: immutable.List[(String, Int)] = wordList.map(
      (word: String) => {
        (word, 1)
      }
    )
    println(tupleList)
    // 4.通过单词进行分组
    val groupList = tupleList.groupBy(
      (kv: (String, Int)) => {
        kv._1
      }
    )
    println(groupList)
    // 5.计算分组后的集合，求出单词出现的频率
    val wordCount = groupList.mapValues(
      list => {
        list.size
      }
    )
    println(wordCount)
    // 6.根据数量排序，获取前二
    val orderByList = wordCount.toList.sortBy(
      kv => kv._2
    )(Ordering.Int)

    println(orderByList)
    println(orderByList.take(2))


    // wordCount简化版本
    dataList
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(_._1)
      .mapValues(_.size)
      .toList
      .sortBy(_._2)(Ordering.Int.reverse)
      .take(2)
      .foreach(println)
  }
}