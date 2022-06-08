package com.atguigu.spark.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Redis工具类
 *
 * @author pangzl
 * @create 2022-06-08 11:26
 */
object MyRedisUtils {

  var jedisPool: JedisPool = null

  def getRedisClient: Jedis = {
    if (jedisPool == null) {
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) // 最大连接数
      jedisPoolConfig.setMaxIdle(20) // 最大空闲时间
      jedisPoolConfig.setMinIdle(20) // 最小空闲时间
      jedisPoolConfig.setBlockWhenExhausted(true) // 忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) // 忙碌时等待时长：毫秒
      jedisPoolConfig.setTestOnBorrow(true) // 每次获得连接的进行测试
      jedisPool = new JedisPool(
        jedisPoolConfig,
        PropertiesUtils("redis.host"),
        PropertiesUtils("redis.port").toInt
      )
    }
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {
    println(getRedisClient.ping())
  }
}
