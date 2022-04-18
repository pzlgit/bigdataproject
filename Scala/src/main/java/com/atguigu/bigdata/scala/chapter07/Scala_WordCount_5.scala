package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 练习： 不同省份的商品点击排行
 *
 * @author pangzl
 * @create 2022-04-17 20:53
 */
object Scala_WordCount_5 {

  def main(args: Array[String]): Unit = {
    val list = List(
      ("zhangsan", "河北", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "鞋"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "鞋"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "鞋"),
      ("zhangsan", "河北", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "帽子"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "电脑"),
      ("zhangsan", "河南", "鞋"),
      ("lisi", "河南", "衣服"),
      ("wangwu", "河南", "电脑"),
      ("zhangsan", "河南", "电脑"),
      ("lisi", "河北", "衣服"),
      ("wangwu", "河北", "帽子")
    )

    // 按照省份名称分组
    val groupProvinceMap = list.groupBy(
      (data: (String, String, String)) => {
        data._2
      }
    )
    // 在按照省份名称分组的基础下,按照商品分组
    val result = groupProvinceMap.mapValues(
      dataList => {
        val productGroupMap = dataList.groupBy(_._3)
        productGroupMap
          .mapValues(_.size)
          .toList
          .sortBy(_._2)(Ordering.Int.reverse)
      }
    )
    println(result)

  }
}