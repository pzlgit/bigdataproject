package com.atguigu.window;

import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;

/**
 * 时间窗口测试
 *
 * @author pangzl
 * @create 2022-06-20 15:35
 */
public class TimeWindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .keyBy(e -> e.user)
                .window(GlobalWindows.create());
        env.execute();
    }
}
