package com.atguigu.spark.bean

/**
 *
 * @author pangzl
 * @create 2022-06-07 19:44
 */
case class PageDisplayLog(
                           mid: String,
                           user_id: String,
                           province_id: String,
                           channel: String,
                           is_new: String,
                           model: String,
                           operate_system: String,
                           version_code: String,
                           brand: String,
                           page_id: String,
                           last_page_id: String,
                           page_item: String,
                           page_item_type: String,
                           during_time: Long,
                           source_type: String,
                           display_type: String,
                           display_item: String,
                           display_item_type: String,
                           display_order: String,
                           display_pos_id: String,
                           ts: Long
                         ) {
}