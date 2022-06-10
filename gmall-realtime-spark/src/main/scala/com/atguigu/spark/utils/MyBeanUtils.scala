package com.atguigu.spark.utils

/**
 * Bean对象工具类
 *
 * @author pangzl
 * @create 2022-06-10 20:56
 */

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 实现对象属性拷贝
 */
object MyBeanUtils {

  /**
   * 将srcObj中属性的值拷贝到destObj对应的属性上
   */
  def copyProperties(srcObj: AnyRef, destObj: AnyRef): Unit = {
    if (srcObj == null || destObj == null) {
      return
    }
    // 获取到srcObj中所有的属性
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields

    // 处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable {

        // Scala样例类会自动为类中的属性提供get、set方法
        // get : fieldname()    set : fieldname_$eq(参数类型)
        var getMethodName: String = srcField.getName
        var setMethodName: String = srcField.getName + "_$eq"

        // 从srcObj中获取get方法对象
        val getMethod: Method =
          srcObj.getClass.getDeclaredMethod(getMethodName)

        // 从destObj中获取set方法对象
        val setMethod: Method =
          try {
            destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
          } catch {
            case ex: NoSuchMethodException => Breaks.break()
          }

        // 忽略val属性
        val destField: Field =
          destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals(Modifier.FINAL)) {
          Breaks.break()
        }

        // 调用get方法获取到srcObj属性的值，再调用set方法将获取到的属性值赋值给destObj的属性
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }
    }
  }
}
