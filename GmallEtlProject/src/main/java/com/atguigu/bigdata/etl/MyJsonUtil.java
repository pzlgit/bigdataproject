package com.atguigu.bigdata.etl;

import com.alibaba.fastjson.JSON;

/**
 * Json校验工具类
 */
public class MyJsonUtil {

    /**
     * 校验字符串是否是Json
     *
     * @param str 字符串
     * @return 是否是json
     */
    public static boolean isJsonValidate(String str) {
        try {
            JSON.parse(str);
            return Boolean.TRUE;
        } catch (Exception e) {
            return Boolean.FALSE;
        }
    }

}