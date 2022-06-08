package com.atguigu.spark.bigdata.scala.chapter06

/**
 * Scala Import
 *
 * @author pangzl
 * @create 2022-04-15 18:54
 */
object Scala_Import {

  def main(args: Array[String]): Unit = {
    // Import 语法可以在任意位置使用
    //    import java.util.ArrayList
    //    val list = new ArrayList()
    // Scala中可以导包，而不是导类
    //    import java.sql
    //    new sql.Date(2022, 4, 15)
    // Scala中可以在同一行中导入相同包中的多个类，简化代码
    //    import java.util.{HashMap, ArrayList}
    //    val list = new util.ArrayList[String]()
    //    val map = new util.HashMap[String, String]()
    // Scala 中可以屏蔽某个包中的类
    //    import  java.util._
    //    import java.sql.{Date => _,Array => _}
    //    val d = new Date()
    // Scala 中可以给类起别名，简化使用
    //    import java.util.{HashMap =>JavaHashMap}
    //    val map = new JavaHashMap[String, String]()
    // Scala 中可以使用类的绝对路径而不是相对路径
    // new HashMap() 其实是从当前包为基准，导入指定子包中的类，如果找不到，再从顶级包中查找
    //    import _root_.java.util.HashMap
    //    val map = new util.HashMap[String, String]()

  }

  class HashMap {

  }
}