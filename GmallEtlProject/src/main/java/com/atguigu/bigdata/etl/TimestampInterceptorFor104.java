package com.atguigu.bigdata.etl;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 自定义Flume拦截器用于将ts时间戳放入到header中
 */
public class TimestampInterceptorFor104 implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        Map<String, String> headers = event.getHeaders();
        String message = new String(body, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(message);
        Long ts = jsonObject.getLong("ts");
        headers.put("timestamp", String.valueOf(ts * 1000));
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class MyBuilder implements Builder {

        @Override
        public Interceptor build() {
            return new TimestampInterceptorFor104();
        }

        @Override
        public void configure(Context context) {

        }
    }

}