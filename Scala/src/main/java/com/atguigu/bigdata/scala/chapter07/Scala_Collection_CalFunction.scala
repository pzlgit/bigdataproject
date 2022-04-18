package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 计算函数
 *
 * @author pangzl
 * @create 2022-04-17 20:21
 */
object Scala_Collection_CalFunction {

  def main(args: Array[String]): Unit = {
    // 集合  计算函数
    val list = List(1, 2, 3, 4, 5)

    // 集合最小值
    println(list.min)
    // 集合最大值
    println(list.max)
    // 集合求和
    println(list.sum)
    // 集合求乘积
    println(list.product)

    // 集合简化规约
    val reduce = list.reduce((x: Int, y: Int) => x + y)
    val reduce1 = list.reduce(_ + _)
    println("reduce=>" + reduce)
    println("reduce1=>" + reduce1)

    // reduceLeft 集合简化规约左
    val reduceLeft = list.reduceLeft(_ - _)
    println("reduceLeft=>" + reduceLeft)
    // reduceRight 集合简化规约右
    val reduceRight = list.reduceRight(_ - _)
    println(reduceRight)

    // fold 集合折叠,fold的底层就是 foldLeft
    val fold = list.fold(6)(_ - _)
    println(fold)
    val foldLeft = list.foldLeft(6)(_ - _)
    println(foldLeft)

    // foldRight 集合折叠右
    val foldRight = list.foldRight(6)(_ - _)
    println(foldRight)

    // scan 集合扫描，可将fold,foldLeft,foldRight 的执行过程记录下来
    println(list.scan(6)(_ - _))
    println(list.scanLeft(6)(_ - _))
    println(list.scanRight(6)(_ - _))


  }

}