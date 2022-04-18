package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 练习： WordCount2
 *
 * @author pangzl
 * @create 2022-04-17 20:53
 */
object Scala_WordCount_3 {

  def main(args: Array[String]): Unit = {
    val list = List(
      ("Hello World Scala Spark", 4),
      ("Hello World Scala", 3),
      ("Hello World", 2),
      ("Hello", 1),
    )
    val list2 = list.flatMap(
      kv => {
        val line: String = kv._1
        val count: Int = kv._2
        val lineList: Array[String] = line.split(" ")
        lineList.map((_, count))
      }
    )

    val groupList = list2.groupBy((kv) => {
      kv._1
    })

    val wordCount: Map[String, Int] = groupList.mapValues(
      list => {
        list.map(_._2).sum
      }
    )
    println(wordCount)

    val result = wordCount.toList.sortBy(_._2)(Ordering.Int.reverse)
    println(result.take(3))


    //

  }
}