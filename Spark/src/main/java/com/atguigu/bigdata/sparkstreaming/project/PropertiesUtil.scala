package com.atguigu.bigdata.sparkstreaming.project

import jline.internal.InputStreamReader

import java.util.Properties

/**
 * 读取资源文件工具类
 *
 * @author pangzl
 * @create 2022-05-06 18:03
 */
object PropertiesUtil {

  // 读取资源文件内容
  def load(name: String): Properties = {
    val properties = new Properties()
    properties.load(
      new InputStreamReader(
        Thread.currentThread().getContextClassLoader
          .getResourceAsStream(name), "UTF-8"
      )
    )
    properties
  }

}
