package com.atguigu.spark.bean

/**
 *
 * @author pangzl
 * @create 2022-06-07 19:50
 */
object StartLog {
  case class StartLog(
                       mid: String,
                       user_id: String,
                       province_id: String,
                       channel: String,
                       is_new: String,
                       model: String,
                       operate_system: String,
                       version_code: String,
                       brand: String,
                       entry: String,
                       open_ad_id: String,
                       loading_time_ms: Long,
                       open_ad_ms: Long,
                       open_ad_skip_ms: Long,
                       ts: Long
                     )
}
