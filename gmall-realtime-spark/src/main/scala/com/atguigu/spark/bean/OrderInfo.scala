package com.atguigu.spark.bean

/**
 *
 * @author pangzl
 * @create 2022-06-11 16:42
 */
case class OrderInfo(
                      id: Long =0L,
                      province_id: Long=0L,
                      order_status: String=null,
                      user_id: Long=0L,
                      total_amount:  Double=0D,
                      activity_reduce_amount: Double=0D,
                      coupon_reduce_amount: Double=0D,
                      original_total_amount: Double=0D,
                      feight_fee: Double=0D,
                      feight_fee_reduce: Double=0D,
                      expire_time: String =null,
                      refundable_time:String =null,
                      create_time: String=null,
                      operate_time: String=null,
                      // 字段处理获取新字段
                      var create_date: String=null,
                      var create_hour: String=null,
                      // 查询地区维度表
                      var province_name:String=null,
                      var province_area_code:String=null,
                      var province_3166_2_code:String=null,
                      var province_iso_code:String=null,
                      // 查询用户维度表
                      var user_age :Int=0,
                      var user_gender:String=null
                    ) {
}
