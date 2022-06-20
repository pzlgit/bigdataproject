package com.atguigu.watermark;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * 自定义数据源中发送水位线
 *
 * @author pangzl
 * @create 2022-06-20 15:03
 */
public class EmitWatermarkInSourceFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .print();
        env.execute();
    }

    public static class ClickSourceWithWatermark implements SourceFunction<Event> {

        private Boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间
                String username = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);
                // 将数据发送出去,并指定时间戳
                ctx.collectWithTimestamp(event, currTs);
                // 发送水位线
                ctx.emitWatermark(new Watermark(currTs - 1L));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
