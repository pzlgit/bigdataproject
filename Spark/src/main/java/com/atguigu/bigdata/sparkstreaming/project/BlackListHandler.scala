package com.atguigu.bigdata.sparkstreaming.project

import org.apache.spark.streaming.dstream.DStream

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

/**
 * 广告黑名单业务实现
 *
 * @author pangzl
 * @create 2022-05-06 19:01
 */
object BlackListHandler {

  // 样例类，时间 地区 城市 用户id 广告id
  case class Ads_log(timestamp: Long, area: String, city: String,
                     userid: String, adid: String)

  // 定义时间格式化对象
  private val df = new SimpleDateFormat("yyyy-MM-dd")

  // 添加到黑名单业务逻辑
  def addBlackList(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    // 1. 统计当前批次中单日每个用户点击每个广告的次数
    val countDs: DStream[((String, String, String), Long)] = filterAdsLogDStream.map(
      ads => {
        // 1.1 格式化时间，将时间戳转化为日期
        val date: String = df.format(new Date(ads.timestamp))
        // 1.2 构建返回值
        ((date, ads.userid, ads.adid), 1L)
      }
    ).reduceByKey(_ + _)

    // 2.写出到数据库
    countDs.foreachRDD(
      rdd => {
        // 每个分区写出一次
        rdd.foreachPartition(
          iter => {

            // 获取数据库连接
            val connection: Connection = JDBCUtil.getConnection

            // 写出每条数据
            iter.foreach {
              case ((date, userid, adid), cnt) => {

                // 向数据库中更新累加点击数
                JDBCUtil.executeUpdate(
                  connection,
                  """
                    | insert into user_ad_count(dt,userid,adid,count)
                    | values(?,?,?,?)
                    | ON DUPLICATE KEY
                    | Update count = count + ?
                    |""".stripMargin,
                  Array(date, userid, adid, cnt, cnt)
                )

                // 查询user_ad_count表，读取MySQL中点击次数
                val ct: Long = JDBCUtil.getDataFromMysql(
                  connection,
                  """
                    |select count from user_ad_count where dt = ? and userid = ? and adid = ?
                    |""".stripMargin,
                  Array(date, userid, adid)
                )

                // 点击次数 > 30,加入黑名单
                if (ct >= 30) {
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |INSERT INTO black_list (userid) VALUES (?) ON DUPLICATE KEY update userid=?
                      |""".stripMargin,
                    Array(userid, userid)
                  )
                }

              }
            }

            // 关闭数据库连接
            connection.close()
          }
        )
      }
    )

  }

  // 判断用户是否在黑名单中
  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {
    adsLogDStream.filter(
      adsLog => {
        val connection: Connection = JDBCUtil.getConnection
        val isExists: Boolean = JDBCUtil.isExist(
          connection,
          """
            |select * from black_list where userid = ?
            |""".stripMargin,
          Array(adsLog.userid)
        )
        connection.close()
        !isExists
      }
    )
  }

}
