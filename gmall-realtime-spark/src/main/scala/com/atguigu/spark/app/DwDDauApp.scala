package com.atguigu.spark.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.spark.bean.{DauInfo, PageLog}
import com.atguigu.spark.utils.{MyBeanUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * 日活宽表处理
 *
 * @author pangzl
 * @create 2022-06-10 11:08
 */
object DwDDauApp {

  def main(args: Array[String]): Unit = {
    // 1.创建实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2.从Redis中读取Kafka偏移量
    val topic = "DWD_PAGE_LOG"
    val groupId = "dwd_dau_group"
    val offsetMap: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(topic, groupId)

    // 3.接收Kafka数据
    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (offsetMap != null && offsetMap.nonEmpty) {
      // 指定offset位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, offsetMap, groupId)
    } else {
      // 默认offset位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, groupId)
    }

    // 4.读取Kafka偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    kafkaDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.转换数据结构
    val pageLogDStream: DStream[PageLog] = kafkaDStream.map(
      record => {
        val message: String = record.value()
        val pageLog: PageLog = JSON.parseObject(message, classOf[PageLog])
        pageLog
      }
    )

    // 6.活跃用户去重
    // 6.1 自我审查：凡是数据中有last_page_id，说明不是本次会话的第一个页面，直接过滤
    val filterDStreamByLastPage: DStream[PageLog] = pageLogDStream.filter(
      pageLog => {
        pageLog.last_page_id == null
      }
    )
    // 6.2 第三方审查，所有会话的第一个页面去Redis中检查是否存在
    val pageLogFilterDStream: DStream[PageLog] = filterDStreamByLastPage.mapPartitions(
      pageLogIter => {
        val jedis: Jedis = MyRedisUtils.getJedisClient()

        val filterList: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val pageLogList: List[PageLog] = pageLogIter.toList
        println("过滤前 : " + pageLogList.size)

        for (pageLog <- pageLogList) {
          val sdf = new SimpleDateFormat("yyyy-MM-dd")
          val dateStr: String = sdf.format(new Date(pageLog.ts))

          // 使用Redis，类型：set Key:DAU:${date} value：今日活跃的mid集合
          val dauKey: String = s"DAU:$dateStr"
          val ifNew: lang.Long = jedis.sadd(dauKey, pageLog.mid)
          if (ifNew == 1L) {
            jedis.expire(dauKey, 3600 * 24)
            filterList.append(pageLog)
          }
        }

        jedis.close()
        println("过滤后: " + filterList.size)
        filterList.toIterator
      }
    )

    // pageLogFilterDStream.print(10)

    // 7.维度合并
    val dauInfoDStream: DStream[DauInfo] = pageLogFilterDStream.mapPartitions(
      pageLogIter => {
        val jedis: Jedis = MyRedisUtils.getJedisClient
        val dauInfoList: ListBuffer[DauInfo] = ListBuffer[DauInfo]()

        for (pageLog <- pageLogIter) {
          // TODO 用户信息关联
          val dimUserKey = s"DIM:USER_INFO:${pageLog.user_id}"
          val userInfoJson: String = jedis.get(dimUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          // 提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          // 提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          // 生日处理为年龄
          var age: String = null
          if (birthday != null) {
            // 闰年无误差
            val birthdayDate: LocalDate = LocalDate.parse(birthday)
            val nowDate: LocalDate = LocalDate.now()
            val period: Period = Period.between(birthdayDate, nowDate)
            val years: Int = period.getYears
            age = years.toString
          }

          val dauInfo = new DauInfo()
          // 将PageLog的字段信息拷贝到DauInfo中
          MyBeanUtils.copyProperties(pageLog, dauInfo)
          dauInfo.user_gender = gender
          dauInfo.user_age = age

          // TODO 地区维度关联
          val provinceKey: String =
            s"DIM:BASE_PROVINCE:${pageLog.province_id}"
          val provinceJson: String = jedis.get(provinceKey)

          if (provinceJson != null && provinceJson.nonEmpty) {
            val provinceJsonObj: JSONObject =
              JSON.parseObject(provinceJson)
            dauInfo.province_name =
              provinceJsonObj.getString("name")
            dauInfo.province_area_code =
              provinceJsonObj.getString("area_code")
            dauInfo.province_3166_2 =
              provinceJsonObj.getString("iso_3166_2")
            dauInfo.province_iso_code =
              provinceJsonObj.getString("iso_code")
          }

          // TODO 日期补充
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
          val dtDate = new Date(dauInfo.ts)
          val dtHr: String = dateFormat.format(dtDate)
          val dtHrArr: Array[String] = dtHr.split(" ")
          dauInfo.dt = dtHrArr(0)
          dauInfo.hr = dtHrArr(1)

          dauInfoList.append(dauInfo)
        }

        jedis.close()
        dauInfoList.toIterator
      }
    )

    dauInfoDStream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }

}
