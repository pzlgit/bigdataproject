package com.atguigu.spark.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.spark.utils.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

/**
 *
 * @author pangzl
 * @create 2022-06-08 20:40
 */
object BaseDBApp_maxwell {

  /**
   * 业务数据分流
   * 1. 读取Kafka偏移量
   * 2. 接收Kakfa数据
   * 3. 提取Kafka偏移量结束点
   * 4. 转换结构
   * 5. 分流处理
   * 5.1 事实数据分流-> Kafka
   * 5.2 维度数据分流-> Redis
   * 6. 提交偏移量
   */
  def main(args: Array[String]): Unit = {
    // 1.创建实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_app").setMaster("local[3]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 2.从Redis中读取Kafka偏移量
    val topic = "ODS_BASE_DB_M"
    val groupId = "base_db_group"
    val offsetsMap: Map[TopicPartition, Long] = MyOffsetUtils.getOffset(topic, groupId)
    // 判断是否为空
    var kafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (offsetsMap != null && offsetsMap.nonEmpty) {
      // 指定offsets位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, offsetsMap, groupId)
    } else {
      // 默认offsets位置消费
      kafkaDStream = MyKafkaUtils.getKafkaDStream(topic, ssc, groupId)
    }
    // 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    kafkaDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 转化数据结构
    val jsonObjDStream: DStream[JSONObject] = kafkaDStream.map(
      record => {
        val value: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(value)
        jsonObj
      }
    )

    // 数据分流
    // 事实表：以表为基础，操作类型为单位，将一个表的数据分流到多个Topic
    // 维度表：分流到Redis
    jsonObjDStream.foreachRDD(
      rdd => {
        // TODO 配置Redis中读取表清单，可优雅增加或减少表清单数据
        val jedis: Jedis = MyRedisUtils.getJedisClient()
        val dimTableKey: String = "DIM:TABLES"
        val factTableKey: String = "FACT:TABLES"
        val dimTables: util.Set[String] = jedis.smembers(dimTableKey)
        val factTables: util.Set[String] = jedis.smembers(factTableKey)
        println("检查维度表: " + dimTables)
        println("检查事实表: " + factTables)
        jedis.close()

        // TODO 维度表和事实表数据做成广播变量，使其能够在Executor端读取
        val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
        val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)

        rdd.foreachPartition(
          iter => {
            val jedis1: Jedis = MyRedisUtils.getJedisClient()
            for (jsonObj <- iter) {
              // 提取表名
              val tableName: String = jsonObj.getString("table")
              // 提取操作类型
              val optType: String = jsonObj.getString("type")
              val opt: String = optType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null // 其他操作，例如：CREATE、ALTER等DDL操作
              }

              if (opt != null) {
                // 提取修改后的数据data
                val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")

                // TODO 事实表数据处理
                if (factTablesBC.value.contains(tableName)) {
                  // 拆分数据到指定主题DWD_[TABLE_NAME]_[I/U/D]
                  val topicName: String = s"DWD_${tableName.toUpperCase()}_$opt"
                  val key: String = dataJsonObj.getString("id")
                  // 发送事实表数据到Kafka
                  MyKafkaUtils.send(topicName, key, dataJsonObj.toJSONString)
                }

                // TODO 维度表数据处理
                if (dimTablesBC.value.contains(tableName)) {
                  // 存储类型选择：String
                  // key:DIM:[table_name]:[主键] value: 整条数据JSON串
                  val id: String = dataJsonObj.getString("id")
                  val redisKey: String = s"DIM:${tableName.toUpperCase()}:$id"
                  // 将数据存储到Redis
                  jedis1.set(redisKey, dataJsonObj.toJSONString)
                }
              }
            }
            jedis1.close()
            MyKafkaUtils.flush()
          }
        )
        // 提交偏移量
        MyOffsetUtils.saveOffset(topic, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
