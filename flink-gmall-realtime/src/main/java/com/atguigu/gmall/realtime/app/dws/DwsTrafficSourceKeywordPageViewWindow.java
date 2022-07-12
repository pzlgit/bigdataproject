package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.func.KeywordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordBean;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.util.ClickHouseUtil;
import com.atguigu.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流量域来源关键词粒度页面浏览各窗口轻度聚合
 *
 * @author pangzl
 * @create 2022-07-12 11:20
 */
public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {
        // TODO 1.基本环境准备
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度
        env.setParallelism(4);
        // 1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 1.4 注册自定义函数
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        //TODO 2.检查点相关的设置(略)

        //TODO 3.从kafka的dwd_traffic_page_log主题中数据创建动态表，并指定watermar以及提取事件时间字段
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_keyword_group";
        tableEnv.executeSql("create table page_log(\n" +
                " common map<string,string>,\n" +
                " page map<string,string>,\n" +
                " ts bigint,\n" +
                " rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),\n" +
                " WATERMARK FOR rowtime AS rowtime - INTERVAL '3' SECOND\n" +
                ")" + MyKafkaUtil.getKafkaDDL(topic, groupId));

        //TODO 4.过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                " page['item'] fullword,\n" +
                " rowtime\n" +
                "from page_log\n" +
                "where page['item'] is not null and page['last_page_id']='search' and page['item_type']='keyword'");
        tableEnv.createTemporaryView("search_table", searchTable);
        //tableEnv.executeSql("select * from search_table").print();

        //TODO 5.使用自定义函数对搜索的内容进行分词  并将分词函数的结果和表中原有字段进行连接
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,rowtime FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table", splitTable);

        //TODO 6.分组、开窗、聚合计算
        Table KeywordBeanSearch = tableEnv.sqlQuery("select\n" +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n'" +
                GmallConstant.KEYWORD_SEARCH + "' source,\n" +
                "keyword,\n" +
                "count(*) keyword_count,\n" +
                "UNIX_TIMESTAMP()*1000 ts\n" +
                "from split_table\n" +
                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND),keyword");

        //TODO 7.将动态表转换为流
        DataStream<KeywordBean> resultDS = tableEnv.toAppendStream(KeywordBeanSearch, KeywordBean.class);

        //TODO 8.将流中的数据写到ClickHouse表中
        resultDS.print(">>>>");
        resultDS.addSink(ClickHouseUtil.getJdbcSink("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"));

        env.execute();
    }

}
