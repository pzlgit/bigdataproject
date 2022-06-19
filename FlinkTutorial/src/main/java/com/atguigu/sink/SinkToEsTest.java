package com.atguigu.sink;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 输出数据到ES
 *
 * @author pangzl
 * @create 2022-06-19 11:10
 */
public class SinkToEsTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        // 输出数据到Es
        HttpHost httpHost = new HttpHost("hadoop102", 9200, "http");
        ArrayList<HttpHost> list = new ArrayList<>();
        list.add(httpHost);

        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction =
                new ElasticsearchSinkFunction<Event>() {
                    @Override
                    public void process(
                            Event event,
                            RuntimeContext runtimeContext,
                            RequestIndexer requestIndexer
                    ) {
                        HashMap<String, String> hashMap = new HashMap<>();
                        hashMap.put(event.user, event.url);
                        IndexRequest source = Requests.indexRequest()
                                .index("clicks2")
                                .type("type")
                                .source(hashMap, XContentType.JSON);
                        requestIndexer.add(source);
                    }
                };
        stream.addSink(
                new ElasticsearchSink.Builder<Event>(
                        list,
                        elasticsearchSinkFunction
                ).build()
        );
        env.execute();
    }
}
