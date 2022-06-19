package com.atguigu.source;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义并行数据源
 *
 * @author pangzl
 * @create 2022-06-18 19:58
 */
public class ParallelSource implements ParallelSourceFunction<Event> {
    // 定义一个数据生成标识位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        // 定义数据生成配置
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};
        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 每隔1s生成一个点击事件数据，方便观测
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 读取自定义数据源
        DataStreamSource<Event> streamSource = env.addSource(new ParallelSource()).setParallelism(2);
        System.out.println(streamSource.getParallelism());
        streamSource.print("ParallelSource");
        env.execute();
    }
}
