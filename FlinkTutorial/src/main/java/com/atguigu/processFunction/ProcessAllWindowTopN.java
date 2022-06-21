package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

/**
 * 基于全窗口函数实现热门URL TopN问题
 *
 * @author pangzl
 * @create 2022-06-21 18:47
 */
public class ProcessAllWindowTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取数据源
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
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
                );
        // 基于全窗口函数实现TopN
        stream
                .map(new MapFunction<Event, String>() {

                    @Override
                    public String map(Event value) throws Exception {
                        return value.url;
                    }
                })
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(
                        new ProcessAllWindowFunction<String, String, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                                // 定义一个HashMap，用于存储数据
                                HashMap<String, Long> map = new HashMap<>();
                                // 遍历窗口中的数据
                                elements.forEach(url -> {
                                    if (map.containsKey(url)) {
                                        Long count = map.get(url);
                                        map.put(url, count + 1);
                                    } else {
                                        map.put(url, 1L);
                                    }
                                });
                                // 将map中的数据放入到List集合中，用于后续进行排序
                                ArrayList<Tuple2<String, Long>> list = new ArrayList<>();
                                map.forEach((key, value) -> {
                                    list.add(Tuple2.of(key, value));
                                });
                                // 排序，取前两名
                                list.sort(new Comparator<Tuple2<String, Long>>() {
                                    @Override
                                    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                        return o2.f1.intValue() - o1.f1.intValue();
                                    }
                                });
                                // 取排序后的前两名
                                StringBuilder result = new StringBuilder();
                                result.append("=======================\n");
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
                        }
                ).print();

        env.execute();
    }
}
