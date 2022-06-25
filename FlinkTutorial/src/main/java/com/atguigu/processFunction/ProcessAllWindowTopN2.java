package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 * 全窗口函数实现 TopN
 *
 * @author pangzl
 * @create 2022-06-24 19:40
 */
public class ProcessAllWindowTopN2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据源并指定水位线和时间戳
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event element, long recordTimestamp) {
                                                return element.timestamp;
                                            }
                                        }
                                )
                ).windowAll(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Event, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        HashMap<String, Long> map = new HashMap<>();
                        elements.forEach(e -> {
                            if (map.containsKey(e.url)) {
                                Long count = map.get(e.url);
                                map.put(e.url, count + 1);
                            } else {
                                map.put(e.url, 1L);
                            }
                        });
                        // 将map转化为List,用于做数据排序
                        List<Tuple2<String, Long>> list = new ArrayList<>();
                        map.forEach((k, v) -> {
                            list.add(Tuple2.of(k, v));
                        });
                        list.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });
                        // 取出前两名
                        StringBuilder result = new StringBuilder();
                        for (int i = 0; i < 2; i++) {
                            Tuple2<String, Long> temp = list.get(i);
                            String info = "浏览量No." + (i + 1) +
                                    " url：" + temp.f0 +
                                    " 浏览量：" + temp.f1 +
                                    " 窗口结束时间：" +
                                    new Timestamp(context.window().getEnd()) + "\n";
                            result.append(info);
                        }
                        result.append("==========================\n");
                        out.collect(result.toString());
                    }
                }).print();
        env.execute();
    }
}
