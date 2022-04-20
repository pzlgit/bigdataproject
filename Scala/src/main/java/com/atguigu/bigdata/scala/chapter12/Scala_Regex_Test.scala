package com.atguigu.bigdata.scala.chapter12

/**
 * Scala 正则表达式 实操
 *
 * @author pangzl
 * @create 2022-04-20 19:08
 */
object Scala_Regex_Test {

  def main(args: Array[String]): Unit = {
    // 匹配手机号表达式验证
    println(isMobileNumber("13473898076"))
    // 提取邮箱地址的域名部分
    val r =
      """([_A-Za-z0-9-]+(?:\.[_A-Za-z0-9-\+]+)*)(@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*(?:\.[A-Za-z]{2,})) ?""".r
    println(r.replaceAllIn("abc.edf+jianli@gmail.com   hello@gmail.com.cn", (m => "*****" + m.group(2))))
  }

  def isMobileNumber(number: String): Boolean = {
    val regex = "^((13[0-9])|(14[5,7,9])|(15[^4])|(18[0-9])|(17[0,1,3,5,6,7,8]))[0-9]{8}$".r
    println(number.slice(number.length - 11, number.length))
    regex.findFirstIn(number.slice(number.length - 11, number.length)) != None
  }


}
