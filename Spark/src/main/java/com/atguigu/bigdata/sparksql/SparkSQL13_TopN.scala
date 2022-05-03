package com.atguigu.bigdata.sparksql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * @author pangzl
 * @create 2022-05-03 20:45
 */
object SparkSQL13_TopN {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .enableHiveSupport()
      .getOrCreate()

    // 1. 查询出所有的点击记录，和城市表产品表做join连接
    spark.sql(
      """
        |select
        |  u.click_product_id,
        |  c.city_name,
        |  c.area,
        |  p.product_name
        |from  user_visit_action u
        |join product_info p
        |on u.click_product_id = p.product_id
        |join  city_info c
        |on u.city_id = c.city_id
        |where u.click_product_id > -1;
        |""".stripMargin).createOrReplaceTempView("t1")
    spark.udf.register("city_remark", functions.udaf(new CityRemarkUDAF()))
    // 2. 根据area和product_name商品名称分组
    spark.sql(
      """
        |select
        |  t1.area,
        |  t1.product_name,
        |   count(*) as click_count,
        |   city_remark(city_name)
        |  from
        |t1 group by t1.area,t1.product_name
        |""".stripMargin).createOrReplaceTempView("t2")
    // 3. 每个区域内排序

    spark.sql(
      """
        |select
        |   *,
        |   rank() over(partition by t2.area order by t2.click_count desc) rk
        |from t2
        |""".stripMargin).createOrReplaceTempView("t3")
    // 4. 取前三
    spark.sql(
      """
        |select
        |*
        |  from t3
        |  where rk <= 3;
        |""".stripMargin).show()

    spark.stop()
  }

  case class Buff(var total: Long, var cityMap: mutable.Map[String, Long])

  class CityRemarkUDAF extends Aggregator[String, Buff, String] {
    // 缓冲区初始化
    override def zero: Buff = {
      Buff(0L, mutable.Map[String, Long]())
    }

    // 缓冲区数据聚合
    override def reduce(b: Buff, city: String): Buff = {
      b.total = b.total + 1
      val newCount: Long = b.cityMap.getOrElse(city, 0L) + 1
      b.cityMap.update(city, newCount)
      b
    }

    // 合并所有的缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      // 合并Map
      b2.cityMap.foreach {
        case (k, v) => {
          val newCnt: Long = b1.cityMap.getOrElse(k, 0L) + v
          b1.cityMap.update(k, newCnt)
        }
      }
      b1
    }

    // 最终返回值
    override def finish(reduction: Buff): String = {
      val remarkList: ListBuffer[String] = ListBuffer[String]()
      // 将统计的城市点击数量的集合进行排序，并取出前2名
      val sortList: List[(String, Long)] = reduction.cityMap.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)
      var sum: Long = 0L
      // 计算前两名的百分比
      sortList.foreach {
        case (city, cnt) => {
          val rate: Long = cnt * 100 / reduction.total
          remarkList.append(city + " " + rate + "%")
          sum += rate
        }
      }
      // 如果城市个数大于2，用其他表示
      if (reduction.cityMap.size > 2) {
        remarkList.append("其他 " + (100 - sum) + "%")
      }
      remarkList.mkString(",")
    }


    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
