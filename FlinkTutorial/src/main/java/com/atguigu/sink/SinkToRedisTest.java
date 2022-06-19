package com.atguigu.sink;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * 将数据输出到Redis
 *
 * @author pangzl
 * @create 2022-06-19 11:05
 */
public class SinkToRedisTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .build();
        env.addSource(new ClickSource())
                .addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<Event>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.HSET, "clicks");
                    }
                    @Override
                    public String getKeyFromData(Event event) {
                        return event.user;
                    }
                    @Override
                    public String getValueFromData(Event event) {
                        return event.url;
                    }
                }));
        env.execute();
    }
}
