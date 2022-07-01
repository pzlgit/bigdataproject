package com.atguigu.gmall.realtime.app.func;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * DIM数据写出到HBase
 *
 * @author pangzl
 * @create 2022-07-01 19:37
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    // 将数据写出到HBase表中
    // jonObj:{"tm_name":"xzls11","sink_table":"dim_base_trademark","id":12}
    @Override
    public void invoke(JSONObject jsonObj, Context ctx) throws Exception {
        // 获取要写入HBase的那张表中
        String sinkTable = jsonObj.getString("sink_table");
        // 删除SinkTable属性
        jsonObj.remove("sink_table");

        // 拼接HBase SQL
        String sql = "upsert into " + GmallConfig.PHOENIX_SCHEMA + "." + sinkTable
                + "(" + StringUtils.join(jsonObj.keySet(), ",") + ") " +
                " values('" + StringUtils.join(jsonObj.values(), "','") + "')";
        System.out.println("向Phoenix表中插入数据的sql：" + sql);

        // 执行SQL
        DruidPooledConnection connection = druidDataSource.getConnection();
        PhoenixUtil.executeSQL(sql, connection);
    }

}
