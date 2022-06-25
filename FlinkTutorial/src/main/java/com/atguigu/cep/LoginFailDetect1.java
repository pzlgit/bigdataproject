package com.atguigu.cep;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * 连续登录失败复杂事件检测
 *
 * @author pangzl
 * @create 2022-06-25 9:15
 */
public class LoginFailDetect1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取登录事件流
        SingleOutputStreamOperator<LoginEvent> stream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }
                        )
        );

        // 定义复杂事件pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("login-fail")
                .where(
                        new SimpleCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent value) throws Exception {
                                return value.eventType.equals("fail");
                            }
                        }
                )
                .times(3)
                .consecutive();
        // 将pattern应用于数据流上
        CEP.pattern(stream.keyBy(r -> r.userId), pattern)
                .select(
                        new PatternSelectFunction<LoginEvent, String>() {
                            @Override
                            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                                LoginEvent first = map.get("login-fail").get(0);
                                LoginEvent second = map.get("login-fail").get(1);
                                LoginEvent third = map.get("login-fail").get(2);
                                return first.userId + "连续三次登录失败，登录时间：" + first.timestamp
                                        + ", " + second.timestamp + ", " + third.timestamp;
                            }
                        }
                ).print();
        env.execute();
    }
}
