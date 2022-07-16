package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联接口
 *
 * @author pangzl
 * @create 2022-07-16 23:07
 */
public interface DimJoinFunction<T> {

    void join(T obj, JSONObject dimJsonObj) throws Exception;

    String getKey(T obj);

}
