package com.atguigu.gmall.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * 演示：Flink DataStream 中的 intervalJoin 使用
 *
 * @author pangzl
 * @create 2022-07-07 18:12
 */
public class Test01_IntervalJoin {

    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2.从socket中获取数据
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Emp(
                            Integer.parseInt(fields[0]),
                            fields[1],
                            Integer.parseInt(fields[2]),
                            Long.parseLong(fields[3])
                    );
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Emp>() {
                                            @Override
                                            public long extractTimestamp(Emp emp, long recordTimestamp) {
                                                return emp.getTs();
                                            }
                                        }
                                )
                );

        // TODO 2.从socket中获取数据
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8889)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Dept(
                            Integer.parseInt(fields[0]),
                            fields[1],
                            Long.parseLong(fields[2])
                    );
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Dept>() {
                                            @Override
                                            public long extractTimestamp(Dept dept, long recordTimestamp) {
                                                return dept.getTs();
                                            }
                                        }
                                )
                );

        // TODO 3.两个流进行intervalJoin
        SingleOutputStreamOperator<Tuple2<Emp, Dept>> intervalJoinDS = empDS.keyBy(Emp::getDeptno)
                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-3), Time.milliseconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp emp, Dept dept, Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {
                        out.collect(Tuple2.of(emp, dept));
                    }
                });

        intervalJoinDS.print(">>");

        env.execute();
    }
}
