package com.atguigu.gmall.test;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 演示： Flink SQL 中的 Join 操作
 *
 * @author pangzl
 * @create 2022-07-07 18:28
 */
public class Test02_FlinkSQLJoin {

    public static void main(String[] args) {
        // TODO 1.获取流执行环境和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置状态的存活时间,过10S后状态就会被清空
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // TODO 2.使用socket读取端口数据，并进行类型转换
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                            String[] fields = line.split(",");
                            return new Emp(
                                    Integer.parseInt(fields[0]),
                                    fields[1],
                                    Integer.parseInt(fields[2]),
                                    Long.parseLong(fields[3])
                            );
                        }
                );
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8889)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Dept(
                            Integer.parseInt(fields[0]),
                            fields[1],
                            Long.parseLong(fields[2])
                    );
                });

        // TODO 3.将数据流转化为动态表
        tableEnv.createTemporaryView("emp", empDS);
        tableEnv.createTemporaryView("dept", deptDS);

        // TODO 4.内连接测试
        // FlinkSQL对于参与连接的两张表底层维护两个状态，默认情况下永不过期，在实际的生产环境中，我们必须设置状态的TTL存活时间，防止状态中的数据过大
        //tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e inner join dept d on e.deptno = d.deptno").print();

        // TODO 5.左外连接测试
        /**
         * 当左表数据到来时，没有找到对应的右表信息，这时就会生成一条数据： +I 左表 + null
         * 当右表数据到来时，能够和左表数据进行关联，这时会生成： -D 左表 + null
         * 同时还会生成一条数据： +I 左表 + 右表 的数据，这种动态表生成的流被称为回撤流。
         * 右表状态的TTL时间仍是10s,左表是主表，TTL时间是每次读或者write的时候都会更新ttl存活时间
         */
        //tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno").print();

        // TODO 6.右外连接测试
        // tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e right join dept d on e.deptno = d.deptno").print();

        // TODO 7.全外连接测试
        tableEnv.executeSql("select e.empno,e.ename,d.deptno,d.dname from emp e full join dept d on e.deptno = d.deptno").print();

        // TODO 8.将左连接数据写出到Kafka
        tableEnv.executeSql("CREATE TABLE kafka_emp (\n" +
                "  empno Integer,\n" +
                "  ename String,\n" +
                "  deptno Integer,\n" +
                "  dname String,\n" +
                "  PRIMARY KEY (empno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'first',\n" +
                "   'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        // 将数据插入到Kafka的主题
        tableEnv.executeSql("insert into kafka_emp select e.empno,e.ename,d.deptno,d.dname from emp e left join dept d on e.deptno = d.deptno");


    }
}
