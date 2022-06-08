package com.atguigu.spark.bigdata.etl;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 自定义拦截器进行Json数据校验
 */
public class MyEtlInterceptor implements Interceptor {

    /**
     * 初始化
     */
    @Override
    public void initialize() {

    }

    /**
     * 单个Event核心业务逻辑
     *
     * @param event 事件
     */
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String str = new String(body, StandardCharsets.UTF_8);
        if (MyJsonUtil.isJsonValidate(str)) {
            return event;
        } else {
            return null;
        }
    }

    /**
     * 批量Event核心业务逻辑
     *
     * @param list 事件集合
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        Iterator<Event> iterator = list.iterator();
        while (iterator.hasNext()) {
            Event event = iterator.next();
            Event intercept = intercept(event);
            if (intercept == null) {
                iterator.remove();
            }
        }
        return list;
    }

    /**
     * 结尾工作
     */
    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder {

        @Override
        public Interceptor build() {
            return new MyEtlInterceptor();
        }

        @Override
        public void configure(Context context) {
        }

    }

}