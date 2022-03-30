package com.atguigu.bigdata.flume;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * 自定义Flume拦截器
 */
public class MyInterceptor implements Interceptor {

    /**
     * 资源初始化工作
     */
    public void initialize() {

    }

    /**
     * 单条Event处理
     */
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        String content =  new String(body);
        if(content.contains("java")){
            headers.put("title","java");
        }else {
            headers.put("title","others");
        }
        event.setHeaders(headers);
        return event;
    }

    /**
     * 多条Event处理
     */
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    /**
     * 资源收尾结束工作
     */
    public void close() {

    }

    public static class MyBuilder implements Builder{

        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {

        }
    }

}