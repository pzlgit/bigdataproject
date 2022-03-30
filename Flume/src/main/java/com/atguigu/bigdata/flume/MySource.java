package com.atguigu.bigdata.flume;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义Source，获取Conf中的自定义配置数据做对应的业务逻辑
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    /**
     * 配置文件中获取数据
     */
    private Long delay;
    private String field;

    public Status process() throws EventDeliveryException {
        // 创建事件头部信息
        Map<String, String> header = new HashMap<String, String>();
        // 创建事件
        SimpleEvent event = new SimpleEvent();
        // 循环封装事件
        for (int i = 0; i < 5; i++) {
            header.put("title", String.valueOf(i));
            // 给事件设置头部信息和Body内容
            event.setHeaders(header);
            event.setBody((field + i).getBytes());
            // 将事件写入到Channel
            getChannelProcessor().processEvent(event);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return Status.BACKOFF;
            }
        }
        return Status.READY;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    /**
     * 初始化配置信息
     */
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field");
    }

}