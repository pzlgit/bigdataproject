package com.atguigu.bigdata.scala.chapter07

/**
 * Scala 功能函数
 *
 * @author pangzl
 * @create 2022-04-17 20:24
 */
object Scala_Function {

  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5)

    // 集合映射map
    val list1 = list.map((num: Int) => {
      num * 2
    })
    val list2 = list.map(_ * 2)
    println(list1)

    // 集合扁平化 flatten
    val list3 = List(
      List(1, 2),
      List(3, 4)
    )
    println(list3.flatten)

    // 集合扁平化映射，可实现自定义扁平化规则
    val list4 = List("Hello Scala", "Hi BigData")
    val list5 = list4.flatMap((str: String) => {
      str.split(" ")
    })
    println(list5)

    // 集合过滤数据，按照指定的条件将数据集中的数据进行筛选过滤
    val list6 = list.filter(
      (num: Int) => {
        num % 2 == 0
      }
    )
    println(list6)

    // 集合分组数据，将数据集中的每一条数据按照指定的规则进行分组，执行结果返回Map集合
    val map: Map[Int, List[Int]] = list.groupBy(
      (num: Int) => {
        num % 2
      }
    )
    println(map)

    println(list.groupBy(_ % 2))

    // 集合排序
    val list7 = List(5, 2, 6, 4, 10)
    println(list7.sortBy(num => num)(Ordering.Int.reverse))
    println(list7.sortWith((left, right) => {
      left < right
    }))

    // mapValues
    val map1 = Map("a" -> 1, "b" -> 2)
    val map2 = map1.mapValues(_ * 2)
    println(map2)
    println("mapValues =>" + map1.mapValues(_ * 2))

    // 对象进行排序
    val user1 = new User10()
    val user2 = new User10()
    val user3 = new User10()
    val user4 = new User10()
    user1.age = 10
    user1.level = 1
    user2.age = 30
    user2.level = 4
    user3.age = 20
    user3.level = 3
    user4.age = 30
    user4.level = 3

    val listUser = List(user1, user2, user3, user4)
    val list8 = listUser.sortWith((left, right) => {
      if (left.age < right.age) {
        true
      } else if (left.age == right.age) {
        left.level > right.level
      } else {
        false
      }
    })
    println(list8)

    // 也可以采用 Tuple 元组排序
    val resultList = list8.sortBy(user =>
      (user.age, user.level)
    )(Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int))
    println(resultList)


  }

}

class User10 {
  var age: Int = _
  var level: Int = _

  override def toString = s"[User10]($age)($level)"
}