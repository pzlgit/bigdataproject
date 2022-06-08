package com.atguigu.spark.bigdata.sparkcore.rdd.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author pangzl
 * @create 2022-04-27 19:32
 */
object RDD_SortBy_Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 设置key为自定义User按照年龄正序排序
    val rdd = sc.makeRDD(List(
      (new User("a", 28), 8),
      (new User("b", 10), 8),
      (new User("c", 18), 8)
    ))
    rdd.sortByKey().collect().foreach(println)
    sc.stop()
  }


}

case class User(name: String, age: Int) extends Ordered[User] {

  override def compare(that: User): Int = {
    if (this.age > that.age) {
      1
    } else if (this.age < that.age) {
      -1
    } else {
      0
    }
  }
}