package com.atguigu.bigdata.sparkstreaming.project

import com.atguigu.bigdata.sparkstreaming.project.BlackListHandler.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

import java.text.SimpleDateFormat
import java.util.Date

/**
 *
 * @author pangzl
 * @create 2022-05-06 20:11
 */
object LastHourAdCountHandler {

  // 时间格式化对象
  private val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm")

  // 过滤后的数据集，统计最近一小时(2分钟)广告分时点击总数
  def getAdHourMintToCount(filterAdsLogDStream: DStream[Ads_log]):
  DStream[(String, List[(String, Long)])] = {
    // 1.开窗 => 时间间隔为1小时
    val windowDstream: DStream[Ads_log] = filterAdsLogDStream.window(Minutes(2))
    // 2.变换数据结构
    val tupleDstream: DStream[((String, String), Long)] = windowDstream.map(
      adsLog => {
        val hm: String = sdf.format(new Date(adsLog.timestamp))
        ((adsLog.adid, hm), 1L)
      }
    )
    // 3.统计总数
    val countDstream: DStream[((String, String), Long)] = tupleDstream.reduceByKey(_ + _)
    // 4.转换数据结构((adid,hm),sum)=>(adid,(hm,sum))
    val resultDS: DStream[(String, (String, Long))] = countDstream.map {
      case ((adid, hm), count) => {
        (adid, (hm, count))
      }
    }
    // 5.按照adid分组排序
    val result: DStream[(String, List[(String, Long)])] = resultDS.groupByKey()
      .mapValues(
        iter => {
          iter.toList.sortBy(_._2)(Ordering.Long.reverse)
        }
      )
    result
  }

}
