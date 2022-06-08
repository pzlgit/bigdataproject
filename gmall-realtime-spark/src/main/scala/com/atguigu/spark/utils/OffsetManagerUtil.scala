package com.atguigu.spark.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
 * Kafka Offset 管理
 *
 * @author pangzl
 * @create 2022-06-08 11:31
 */
object OffsetManagerUtil {

  /**
   * 向Redis中保存偏移量
   * Kafka中存储Offset的信息： groupId + topicName + partition => offset
   * Redis中存储Offset的格式： type => Hash 永不过期
   * key:offset:[topicName]:[groupId] field:partition value=>offset
   *
   * @param topicName    主题名称
   * @param groupId      消费者组名称
   * @param offsetRanges 分区的偏移量起始位置和结束位置
   */
  def saveOffset(topicName: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    // 构建Map,用于向Redis中存储数据
    val offsetMap = new util.HashMap[String, String]()

    // 对封装了Kafka偏移量的数据集合进行遍历
    for (offsetRange <- offsetRanges) {
      // 获取分区
      val partition: Int = offsetRange.partition
      // 获取结束点
      val offset: Long = offsetRange.untilOffset
      // 将数据设置到Map中
      offsetMap.put(partition.toString, offset.toString)
      // 打印测试
      println("保存分区:" + partition + ":" + offsetRange.fromOffset +
        "--->" + offsetRange.untilOffset)
    }

    // 如果要保存的偏移量不为空，那么就执行保存操作
    if (offsetMap != null && offsetRanges.length > 0) {
      // 获取Redis客户端连接
      val redisClient: Jedis = MyRedisUtils.getRedisClient
      // 保存数据到Redis中
      // 构建RedisKey
      val redisKey = s"offset:$topicName:$groupId"
      redisClient.hmset(redisKey, offsetMap)
      redisClient.close()
    }
  }


  /**
   * 从Redis中读取偏移量
   * Kafka中存储的Offset的信息： group+topic+partition (GTP) => offset
   * Redis中存储的Offset的格式： type => Hash 永不过期
   * key: offset:[topicName]:[groupId] filed:partition value:offset
   *
   * @param topicName 主题名称
   * @param groupId   消费者组名称
   * @return 对应的分区的偏移量信息
   */
  def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
    // 获取Redis客户端对象
    val redisClient: Jedis = MyRedisUtils.getRedisClient
    // 连接RedisKey
    val redisKey: String = s"offset:$topicName:$groupId"
    // 读取数据
    val offsetMap: util.Map[String, String] = redisClient.hgetAll(redisKey)
    // 将Java的Mao转化为Scala的Map,方便后续操作
    import scala.collection.JavaConverters._
    val kafkaOffsetMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partitionId, offset) => {
        println("读取分区偏移量：" + partitionId + ":" + offset)
        // 将Redis中的分区信息偏移量转化为元组
        (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
      }
    }.toMap
    redisClient.close()
    kafkaOffsetMap
  }

}