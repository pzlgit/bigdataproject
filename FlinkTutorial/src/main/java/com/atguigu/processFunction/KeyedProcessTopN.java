package com.atguigu.processFunction;

import com.atguigu.bean.Event;
import com.atguigu.bean.UrlViewCount;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 实现热门URL TopN
 *
 * @author pangzl
 * @create 2022-06-21 19:00
 */
public class KeyedProcessTopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 基于url分区求总量，并封装窗口信息
        SingleOutputStreamOperator<UrlViewCount> stream2 = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                ).keyBy(r -> r.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        // 按照timeWindowEnd分组
        stream2.keyBy(r -> r.windowEnd)
                .process(new TopN(2))
                .print();
        env.execute();
    }

    // 定义窗口函数，实现topN
    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private Integer n;

        public TopN(int n) {
            this.n = n;
        }

        // 定义一个列表状态，用于存放所有数据
        private ListState<UrlViewCount> urlViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>(
                            "url-view-count-list",
                            Types.POJO(UrlViewCount.class)
                    )
            );
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 将count数据添加到列表状态中保存起来
            urlViewCountListState.add(value);
            // 注册一个window end + 1L 的定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器定时执行操作，用于获取列表中的数据进行排序
            // 收集数据
            ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCounts.add(urlViewCount);
            }
            // 清空列表中的状态
            urlViewCountListState.clear();
            // 集合中数据排序
            urlViewCounts.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });
            // 取前两名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount UrlViewCount = urlViewCounts.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + UrlViewCount.url + " "
                        + "浏览量：" + UrlViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());

        }
    }

    // 定义聚合函数，求url访问总量
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            // 创建累加器，初始定义0
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            // 每来一条数据就执行一次add
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    // 定义窗口函数，封装窗口信息
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            // 结合窗口信息，包装输出内容
            out.collect(new UrlViewCount(s, elements.spliterator().getExactSizeIfKnown(),
                    context.window().getStart(), context.window().getEnd()));
        }
    }
}
