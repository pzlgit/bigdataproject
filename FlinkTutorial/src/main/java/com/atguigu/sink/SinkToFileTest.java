package com.atguigu.sink;

import com.atguigu.bean.Event;
import com.atguigu.source.ClickSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * 将数据写入文件
 *
 * @author pangzl
 * @create 2022-06-19 10:49
 */
public class SinkToFileTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        // 基于行编码模式构建
        StreamingFileSink<String> fileSink = StreamingFileSink.<String>forRowFormat(
                new Path("./output"), new SimpleStringEncoder<String>("UTF-8")
        ).withRollingPolicy(
                // 至少包含15分钟的数据
                // 最近5分钟没有收到新的数据
                // 文件大小已达到1 GB
                DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build()
        ).build();
        stream.map(Event::toString)
                .addSink(fileSink);
        env.execute();
    }
}
