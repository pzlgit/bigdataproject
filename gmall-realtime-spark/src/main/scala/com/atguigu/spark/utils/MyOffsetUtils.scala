package com.atguigu.spark.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * Kafka Offsets 管理工具类
 *
 * @author pangzl
 * @create 2022-06-08 19:48
 */
object MyOffsetUtils {

  /**
   * 从Redis中读取偏移量
   * Kafka中存储Offset信息：group+topic+partition(GTP) => offset
   * Redis中存储offset格式：type=>Hash   永不过期
   * [key=>offset:[topic]:[groupId] field=>partitionId value=>偏移量值]
   *
   * @param topicName 主题名称
   * @param groupId   消费者组
   * @return 当前消费者组中，消费的主题对应的分区的偏移量信息
   */
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = MyRedisUtils.getJedisClient()
    val redisKey: String = s"offset:$topicName:$groupId"
    // Redis操作
    val offsetMap: util.Map[String, String] = jedis.hgetAll(redisKey)
    jedis.close()
    // 将Java Map 转换为 Scala Map
    import scala.collection.JavaConverters._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partitionId, offset) => {
        println("读取分区偏移量：" + partitionId + ":" + offset)
        // 格式封装
        (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }
    }.toMap
    kafkaOffsetMap
  }

  /**
   * 向Redis中保存偏移量
   * Kafka中存储Offset信息：group+topic+partition(GTP) => offset
   * Redis中存储offset格式：type=>Hash  永不过期
   * [key=>offset:[topic]:[groupId] field=>partitionId value=>偏移量值]
   *
   * @param topicName    主题名
   * @param groupId      消费者组
   * @param offsetRanges 当前消费者组中，消费的主题对应的分区的偏移量起始和结束信息
   */
  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 定义Map
    val offsetMap = new util.HashMap[String, String]()

    // 对offsetRanges遍历获取每一个offset
    for (offsetRange <- offsetRanges) {
      // 获取Partition
      val partition: Int = offsetRange.partition
      // 获取Offset结束点
      val untilOffset: Long = offsetRange.untilOffset
      // 封装到Map中，用于存储到Redis
      offsetMap.put(partition.toString, untilOffset.toString)
      // 打印测试
      println("保存分区:" + partition + ":" + offsetRange.fromOffset +
        "--->" + offsetRange.untilOffset)
    }

    // Redis操作
    val redisKey: String = s"offset:$topicName:$groupId"
    // 如果需要保存的偏移量不为空，执行保存操作
    if (offsetMap != null && offsetMap.size() > 0) {
      val jedis: Jedis = MyRedisUtils.getJedisClient()
      jedis.hmset(redisKey, offsetMap)
      jedis.close()
    }
  }

}
