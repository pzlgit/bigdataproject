package com.atguigu.spark.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.spark.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.spark.utils.{MyESUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import scala.collection.mutable.ListBuffer

/**
 * 订单宽表处理
 *
 * @author pangzl
 * @create 2022-06-11 16:43
 */
object DwDOrderApp {
  def main(args: Array[String]): Unit = {
    // 1.准备实时环境
    val sparkconf: SparkConf = new SparkConf()
      .setAppName("dwd_dau_app")
      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkconf, Seconds(5))

    val orderInfoTopic = "DWD_ORDER_INFO_I"
    val orderDetailTopic = "DWD_ORDER_DETAIL_I"
    val groupId = "dwd_order_group"

    // 2.从Redis中读取Kafka偏移量
    val orderInfoOffsets: Map[TopicPartition, Long] =
      MyOffsetUtils.getOffset(orderInfoTopic, groupId)
    val orderDetailOffsets: Map[TopicPartition, Long] =
      MyOffsetUtils.getOffset(orderDetailTopic, groupId)

    // 3.接收Kafka数据
    // order_info
    var orderInfoKafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(
        orderInfoTopic, ssc, orderInfoOffsets, groupId)
    } else {
      orderInfoKafkaDStream =
        MyKafkaUtils.getKafkaDStream(orderInfoTopic, ssc, groupId)
    }
    // order_detail
    var orderDeatilKafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsets != null &&
      orderDetailOffsets.nonEmpty) {
      orderDeatilKafkaDStream = MyKafkaUtils.getKafkaDStream(
        orderDetailTopic, ssc, orderDetailOffsets, groupId)
    } else {
      orderDeatilKafkaDStream =
        MyKafkaUtils.getKafkaDStream(orderDetailTopic, ssc, groupId)
    }

    // 4.提取偏移量结束点
    // order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    orderInfoKafkaDStream = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges =
          rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // order_detail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    orderDeatilKafkaDStream = orderDeatilKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges =
          rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.转换结构
    val orderInfoDStream: DStream[OrderInfo] =
      orderInfoKafkaDStream.map(
        record => {
          val jsonStr: String = record.value()
          val orderInfo: OrderInfo =
            JSON.parseObject(jsonStr, classOf[OrderInfo])
          orderInfo
        }
      )
    val orderDetailDStream: DStream[OrderDetail] =
      orderDeatilKafkaDStream.map(
        record => {
          val jsonStr: String = record.value()
          val orderDetail: OrderDetail =
            JSON.parseObject(jsonStr, classOf[OrderDetail])
          orderDetail
        }
      )

    //    orderInfoDStream.print(10)
    //    orderDetailDStream.print(10)

    // 6.订单表维度合并(补充用户年龄性别、地区信息)
    val orderInfoWithDimDStream: DStream[OrderInfo] =
      orderInfoDStream.mapPartitions(
        orderInfoIter => {
          val jedis: Jedis = MyRedisUtils.getJedisClient
          val orderInfoList: List[OrderInfo] = orderInfoIter.toList

          for (orderInfo <- orderInfoList) {
            // 6.1补充用户信息
            val userInfoKey = s"DIM:USER_INFO:${orderInfo.user_id}"
            val userInfoJson: String = jedis.get(userInfoKey)
            val userInfoJsonObj: JSONObject =
              JSON.parseObject(userInfoJson)
            orderInfo.user_gender =
              userInfoJsonObj.getString("gender")
            val birthday: String =
              userInfoJsonObj.getString("birthday")
            val birthdayDate: LocalDate = LocalDate.parse(birthday)
            val nowDate: LocalDate = LocalDate.now()
            val period: Period = Period.between(birthdayDate, nowDate)
            val age: Int = period.getYears
            orderInfo.user_age = age

            // 6.2补充日期字段
            val dateTimeArr: Array[String] =
              orderInfo.create_time.split(" ")
            orderInfo.create_date = dateTimeArr(0)
            orderInfo.create_hour = dateTimeArr(1).split(":")(0)

            // 6.3补充地区信息
            val provinceKey =
              "DIM:BASE_PROVINCE:" + orderInfo.province_id
            val provinceJson: String = jedis.get(provinceKey)
            val provinceJsonObj: JSONObject =
              JSON.parseObject(provinceJson)
            orderInfo.province_name =
              provinceJsonObj.getString("name")
            orderInfo.province_area_code =
              provinceJsonObj.getString("area_code")
            orderInfo.province_3166_2_code =
              provinceJsonObj.getString("iso_3166_2")
            orderInfo.province_iso_code =
              provinceJsonObj.getString("iso_code")
          }

          jedis.close()
          orderInfoList.toIterator
        }
      )

    //    orderInfoWithDimDStream.print(10)
    // 7.双流Join
    // 7.1 如果要做Join操作,数据结构必须是DStream[K,V]和DStream[K,V]
    val orderInfoWithKeyDStream: DStream[(Long, OrderInfo)] =
    orderInfoWithDimDStream.map(orderInfo => (orderInfo.id, orderInfo))

    val orderDetailWithKeyDStream: DStream[(Long, OrderDetail)] =
      orderDetailDStream.map(
        orderDetail => (orderDetail.order_id, orderDetail)
      )

    // Join只能实现同一批次的数据进行Join,如果有数据延迟，延迟的数据就不能Join成功，就会有数据丢失
    // val joinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWithKeyDStream.join(orderDetailWithKeyDStream)
    // joinDStream.print(1000)

    // 7.2 通过状态或者缓存来解决数据延迟导致的数据不在一个批次中的问题
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoWithKeyDStream.fullOuterJoin(orderDetailWithKeyDStream)

    val orderWideDStream: DStream[OrderWide] = orderJoinDStream.flatMap {
      case (orderId, (orderInfoOpt, orderDetailOpt)) => {
        val orderWideList: ListBuffer[OrderWide] =
          ListBuffer[OrderWide]()
        val jedis: Jedis = MyRedisUtils.getJedisClient
        // 1. order_info主表存在
        if (orderInfoOpt != None) {
          val orderInfo: OrderInfo = orderInfoOpt.get
          // 1.1 order_detail从表存在
          if (orderDetailOpt != None) {
            // 将主表和从表合并
            val orderDetail: OrderDetail = orderDetailOpt.get
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          }

          // 1.2 order_info主表写缓存
          // type: String
          // key : ORDER_JOIN:ORDER_INFO:[ID]
          // value : orderInfoJson
          // 写入API: set
          // 读取API: get
          // 过期时间: 小时~天  24小时
          val orderInfoKey =
          s"ORDER_JOIN:ORDER_INFO:${orderInfo.id}"
          val orderInfoJson: String =
            JSON.toJSONString(orderInfo, new SerializeConfig(true))
          jedis.setex(orderInfoKey, 3600 * 24, orderInfoJson)

          // 1.3 order_info主表读缓存查询order_detail
          // type : set
          // key : ORDER_JOIN:ORDER_DETAIL:[ORDERID]
          // value : orderDetailJson
          // 写入API: sadd
          // 读取API: smembers
          // 过期时间： 小时~天 24小时
          val orderDetailKey =
          s"ORDER_JOIN:ORDER_DETAIL:${orderInfo.id}"
          val orderDetaiJsonSet: java.util.Set[String] =
            jedis.smembers(orderDetailKey)
          if (orderDetaiJsonSet != null &&
            orderDetaiJsonSet.size() > 0) {
            import scala.collection.JavaConverters._
            for (orderDetailJson <- orderDetaiJsonSet.asScala) {
              val orderDetail: OrderDetail =
                JSON.parseObject(orderDetailJson, classOf[OrderDetail])
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWideList.append(orderWide)
            }

          }
        } else {
          // 2. order_info主表不存在，order_detail从表存在
          val orderDetail: OrderDetail = orderDetailOpt.get
          // 2.1 order_detail从表读order_info缓存
          val orderInfoKey =
            s"ORDER_JOIN:ORDER_INFO:${orderDetail.order_id}"
          val orderInfoJson: String = jedis.get(orderInfoKey)
          if (orderInfoJson != null && orderInfoJson.nonEmpty) {
            val orderInfo: OrderInfo =
              JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            val orderWide = new OrderWide(orderInfo, orderDetail)
            orderWideList.append(orderWide)
          } else {
            // 2.2 order_detail从表写缓存
            val orderDetailKey =
              s"ORDER_JOIN:ORDER_DETAIL:${orderDetail.order_id}"
            val orderDetailJson: String =
              JSON.toJSONString(orderDetail, new SerializeConfig(true))
            jedis.sadd(orderDetailKey, orderDetailJson)
            jedis.expire(orderDetailKey, 3600 * 24)
          }
        }
        jedis.close()
        orderWideList
      }
    }

    // 8.写入ES
    orderWideDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWideList: List[(String, OrderWide)] =
              orderWideIter.toList
                .map(orderWide =>
                  (orderWide.detail_id.toString, orderWide)
                )
            if (orderWideList.size > 0) {
              val orderWideT: (String, OrderWide) = orderWideList(0)
              val dt: String = orderWideT._2.create_date
              val indexName = s"gmall_order_wide_$dt"
              MyESUtils.bulkSaveIdempotent(orderWideList, indexName)
            }
          }
        )

        // 9.提交偏移量
        MyOffsetUtils.saveOffset(orderInfoTopic, groupId,
          orderInfoOffsetRanges)
        MyOffsetUtils.saveOffset(orderDetailTopic, groupId,
          orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
