package com.atguigu.spark.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.spark.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.atguigu.spark.utils.{MyKafkaUtils, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ODS层数据分流处理
 *
 * @author pangzl
 * @create 2022-06-07 18:51
 */
object BaseLogApp {

  def main(args: Array[String]): Unit = {
    // 1.构建Spark实时环境
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("base_log_app")
      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2.定义各种主题名称
    //  原始主题
    val ods_base_topic: String = "ODS_BASE_LOG"
    //  启动主题
    val dwd_start_log: String = "DWD_START_LOG"
    //  页面访问主题
    val dwd_page_log: String = "DWD_PAGE_LOG"
    //  页面动作主题
    val dwd_page_action: String = "DWD_PAGE_ACTION"
    //  页面曝光主题
    val dwd_page_display: String = "DWD_PAGE_DISPLAY"
    //  错误主题
    val dwd_error_info: String = "DWD_ERROR_INFO"

    //  3.定义消费者组
    val group_id: String = "ods_base_log_group"

    // TODO 4.手动提交偏移量补充
    // 在Redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(ods_base_topic, group_id)
    // 判断数据是否读取到
    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      // 指定Offset位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ods_base_topic, ssc, group_id, offsets)
    } else {
      // 默认位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ods_base_topic, ssc, group_id)
    }

    // 在数据转化前提取本次流中的offset结束点
    var ranges: Array[OffsetRange] = null
    kafkaDStream = kafkaDStream.transform(
      rdd => {
        println(rdd.getClass.getName)
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )


    // 4.消费数据
    //    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
    //      MyKafkaUtils.getKafkaDStream(ods_base_topic, ssc, group_id)
    // 测试是否消费到数据
    //kafkaDStream.map(_.value()).print(3)

    // 5.将Kafka消费到的数据转化为通用JsonObject数据结构
    val jsonObject: DStream[JSONObject] = kafkaDStream.map(
      record => {
        val message: String = record.value()
        println(message)
        val obj: JSONObject = JSON.parseObject(message)
        obj
      }
    )

    // jsonObject.print(3)

    // 6.数据分流处理
    jsonObject.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            // 6.1 分流错误日志，只要错误日志中有err就代表是错误日志，直接发送到Kafka即可
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            if (errObj != null) {
              // 发送错误日志到Kafka
              MyKafkaUtils.send(dwd_error_info, jsonObj.toJSONString)
            } else {
              // 6.2 提取公共信息common和ts
              val commonObj: JSONObject = jsonObj.getJSONObject("common")
              val mid: String = commonObj.getString("mid")
              val ba: String = commonObj.getString("ba")
              val uid: String = commonObj.getString("uid")
              val ar: String = commonObj.getString("ar")
              val ch: String = commonObj.getString("ch")
              val os: String = commonObj.getString("os")
              val md: String = commonObj.getString("md")
              val vc: String = commonObj.getString("vc")
              val isNew: String = commonObj.getString("is_new")
              val ts: Long = jsonObj.getLong("ts")

              // 6.3分流页面日志
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                val pageId: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String =
                  pageObj.getString("item_type")
                val lastPageId: String =
                  pageObj.getString("last_page_id")
                val duringTime: Long = pageObj.getLong("during_time")
                val sourceType: String = pageObj.getString("source_type")
                // 封装Bean
                val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, ba, vc, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                // 页面日志发送到Kafka
                MyKafkaUtils.send(
                  dwd_page_log,
                  JSON.toJSONString(pageLog, new SerializeConfig(true))
                )

                // 6.4 分流动作日志
                val actionArray: JSONArray = jsonObj.getJSONArray("actions")
                if (actionArray != null && actionArray.size() > 0) {
                  for (i <- 0 until actionArray.size()) {
                    val actionObj: JSONObject = actionArray.getJSONObject(i)
                    val actionId: String =
                      actionObj.getString("action_id")
                    val actionItem: String =
                      actionObj.getString("item")
                    val actionItemType: String =
                      actionObj.getString("item_type")
                    val actionTs: Long = actionObj.getLong("ts")
                    // 封装Bean
                    // 封装Bean
                    val pageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId,
                      pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                    // 动作日志发送到Kafka
                    MyKafkaUtils.send(dwd_page_action, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                  }
                }

                // 6.5 分流曝光日志
                val displayArray: JSONArray = jsonObj.getJSONArray("displays")
                if (displayArray != null && displayArray.size() > 0) {
                  for (i <- 0 until displayArray.size()) {
                    val displayObj: JSONObject =
                      displayArray.getJSONObject(i)
                    val displayType: String =
                      displayObj.getString("display_type")
                    val displayItem: String =
                      displayObj.getString("item")
                    val displayItemType: String =
                      displayObj.getString("item_type")
                    val displayOrder: String =
                      displayObj.getString("order")
                    val displayPosId: String =
                      displayObj.getString("pos_id")
                    // 封装Bean
                    val displayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId,
                      pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, displayOrder, displayPosId, ts)
                    // 曝光日志发送到Kafka
                    MyKafkaUtils.send(dwd_page_display, JSON.toJSONString(displayLog, new SerializeConfig(true)))
                  }
                }
              }

              // 6.6分流启动日志
              val startObj: JSONObject = jsonObj.getJSONObject("start")
              if (startObj != null) {
                val entry: String = startObj.getString("entry")
                val loadingTimeMs: Long =
                  startObj.getLong("loading_time_ms")
                val openAdId: String =
                  startObj.getString("open_ad_id")
                val openAdMs: Long = startObj.getLong("open_ad_ms")
                val openAdSkipMs: Long =
                  startObj.getLong("open_ad_skip_ms")

                // 封装Bean
                val startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba,
                  entry, openAdId, loadingTimeMs, openAdMs, openAdSkipMs, ts)

                // 启动日志发送到Kafka
                MyKafkaUtils.send(dwd_start_log,
                  JSON.toJSONString(startLog, new SerializeConfig(true)))
              }

            }

          }
        )
        // foreachRdd里面：driver端执行，一个批次执行一次
        OffsetManagerUtil.saveOffset(ods_base_topic, group_id, ranges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

}