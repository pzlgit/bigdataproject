package com.atguigu.spark.bigdata.sparksql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * @author pangzl
 * @create 2022-05-05 16:23
 */
object SparkSQL13_TopN {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkSQLTest")
      .enableHiveSupport()
      .getOrCreate()

    // 1. join 关联三张表获取数据字段
    spark.sql(
      """
        |select
        |  u.click_product_id,
        |  c.city_name,
        |  c.area,
        |  p.product_name
        |from user_visit_action u
        |join  city_info c on u.city_id = c.city_id
        |join product_info p on u.click_product_id = p.product_id
        |where u.click_product_id  > -1;
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.udf.register("city_remark",functions.udaf(new CityRemarkUDAF))

    // 2. 根据area和商品名称分组
    spark.sql(
      """
        |select
        |  t1.area,
        |  t1.product_name,
        |  count(t1.click_product_id) as click_count,
        |  city_remark(t1.city_name)
        |from t1
        |group by t1.area,t1.product_name;
        |""".stripMargin).createOrReplaceTempView("t2")

    // 3.根据计算的结果进行区域内排序
    spark.sql(
      """
        |select
        |  *,
        |  rank() over(partition by t2.area order by t2.click_count desc) rk
        |from t2;
        |""".stripMargin).createOrReplaceTempView("t3")

    // 4. 根据rk取每个区域内前三名
    spark.sql(
      """
        |select
        | *
        |from t3
        |where t3.rk <= 3;
        |""".stripMargin).show(100, false)


    spark.stop()
  }


  case class Buff(var total: Long, var cityMap: mutable.Map[String, Long])

  class CityRemarkUDAF extends Aggregator[String, Buff, String] {

    // 初始化缓冲区
    override def zero: Buff = Buff(0L, mutable.Map[String, Long]())

    // 数据聚合
    override def reduce(buff: Buff, city: String): Buff = {
      buff.total += 1
      val oldCount: Long = buff.cityMap.getOrElse(city, 0L)
      buff.cityMap.update(city, oldCount + 1)
      buff
    }

    // 合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total += b2.total
      // 两个Map合并
      b2.cityMap.foreach {
        case (city, cnt) => {
          val old: Long = b1.cityMap.getOrElse(city, 0L)
          b1.cityMap.update(city, old + cnt)
        }
      }
      b1
    }

    // 最终输出结果
    override def finish(buff: Buff): String = {
      val remarkList = ListBuffer[String]()

      // 将统计的城市点击量的集合进行排序，取前二
      val top2: List[(String, Long)] = buff.cityMap.toList.sortBy(_._2)(Ordering.Long.reverse).take(2)

      // 用于记录剩余百分比
      var sum: Long = 0L

      top2.foreach {
        case (city, cnt) => {
          val rate: Long = cnt * 100 / buff.total
          remarkList.append(city + " " + rate + "%")
          sum += rate
        }
      }

      // 如果城市个数大于2，用其他表示
      if(buff.cityMap.size >2){
        remarkList.append("其他 "+ (100-sum) + "%" )
      }

      remarkList.mkString(",")

    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING

  }

}
