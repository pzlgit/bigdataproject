package com.atguigu.spark.utils

import java.util.ResourceBundle

/**
 * 读取配置文件工具类
 *
 * @author pangzl
 * @create 2022-06-08 18:42
 */
object PropertiesUtils {

  private val resourceBundle: ResourceBundle = ResourceBundle.getBundle("config")

  /**
   * 根据Key读取配置文件中的值
   */
  def apply(key: String): String = {
    resourceBundle.getString(key)
  }

  /**
   * 测试
   */
  def main(args: Array[String]): Unit = {
    println(PropertiesUtils.apply("kafka.bootstrap.servers"))
    println(PropertiesUtils("kafka.bootstrap.servers"))
  }

}
