package com.atguigu.spark.bigdata.sparkstreaming.project

import com.atguigu.bigdata.sparkstreaming.project.BlackListHandler.Ads_log
import org.apache.spark.streaming.dstream.DStream

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

/**
 * 广告点击实时统计业务实现
 *
 * @author pangzl
 * @create 2022-05-06 19:55
 */
object DateAreaCityAdCountHandler {

  // 时间格式化对象
  private val df = new SimpleDateFormat("yyyy-MM-dd")

  // 根据黑名单过滤后的数据集，统计每天各大区各个城市广告点击总数并保存至MySQL中
  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {
    // 1. 统计每天各区，各城市，广告点击总数
    val countDstream: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(
      adsLog => {
        val dt: String = df.format(new Date(adsLog.timestamp))
        ((dt, adsLog.area, adsLog.city, adsLog.adid), 1L)
      }
    ).reduceByKey(_ + _)

    // 2. 将单个批次的数据集合写入到数据库中
    countDstream.foreachRDD(
      rdd => {

        // 对每个分区的数据单独处理
        rdd.foreachPartition(

        iter => {
          val connection: Connection = JDBCUtil.getConnection
          iter.foreach {
            case ((dt, area, city, adid), cnt) => {
              JDBCUtil.executeUpdate(
                connection,
                """
                  |INSERT INTO area_city_ad_count (dt,area,city,adid,count)
                  |VALUES(?,?,?,?,?)
                  |ON DUPLICATE KEY
                  |UPDATE count=count+?;
                        """.stripMargin,
                Array(dt, area, city, adid, cnt, cnt)
              )
            }
          }
          connection.close()
        }
        )

      }
    )

  }


}
