package com.atguigu.source;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源（用户访问页面操作数据生成）
 *
 * @author pangzl
 * @create 2022-06-17 20:38
 */
public class ClickSource implements SourceFunction<Event> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (running) {
            ctx.collect(
                    new Event(
                            users[random.nextInt(users.length)],
                            urls[random.nextInt(urls.length)],
                            Calendar.getInstance().getTimeInMillis()
                    )
            );
            // 每隔一秒生成一条Event数据，便于观测
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
