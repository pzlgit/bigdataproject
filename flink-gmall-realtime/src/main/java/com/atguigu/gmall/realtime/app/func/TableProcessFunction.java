package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.util.DruidDSUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * 自定义函数实现业务流和广播流连结后的处理逻辑
 *
 * @author pangzl
 * @create 2022-07-01 18:20
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取连接池对象
        druidDataSource = DruidDSUtil.createDataSource();
    }

    // 主流业务数据处理逻辑
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 获取广播流
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 从数据中获取业务数据表名
        String tableName = jsonObj.getString("table");
        // 从广播流中获取对应的配置信息
        TableProcess tableProcess = broadcastState.get(tableName);
        if (tableProcess != null) {
            // 如果能查询到，就是维度数据
            // 获取对应数据中的字段data数据
            JSONObject data = jsonObj.getJSONObject("data");
            // 过滤不需要的属性
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(sinkColumns, data);
            // 增加sink_table属性，用于标识数据写入那个表
            data.put("sink_table", tableProcess.getSinkTable());
            // 将数据向下游传递
            out.collect(data);
        } else {
            // 查询不到，就是非维度数据
            System.out.println(tableName + "不是维度数据");
        }
    }

    // 过滤掉字符串中不需要的属性
    private void filterColumn(String sinkColumns, JSONObject data) {
        String[] columnArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnArr);
        data.entrySet().removeIf(key -> !columnList.contains(key.getKey()));
    }

    // 广播流数据处理逻辑(数据不变化就不会执行此方法)
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        // 将配置流中的数据转化数据格式为JSONObject
        JSONObject jsonObj = JSONObject.parseObject(jsonStr);

        // 获取广播状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        // 获取当前流数据中的操作类型（c/u/d）
        String op = jsonObj.getString("op");
        if ("d".equals(op)) {
            // 删除操作,数据中before有数据值，after为null
            TableProcess before = jsonObj.getObject("before", TableProcess.class);
            String sourceTable = before.getSourceTable();
            // 删除广播状态中对应的业务表的数据
            broadcastState.remove(sourceTable);
        } else {
            // 修改或者更新操作,数据中只拿到after中的数据就是最新的数据
            TableProcess after = jsonObj.getObject("after", TableProcess.class);

            // 获取创建HBase的信息，包括目标表名称、字段、主键、扩展
            String sinkTable = after.getSinkTable();
            String sinkColumns = after.getSinkColumns();
            String sinkExtend = after.getSinkExtend();
            String sinkPk = after.getSinkPk();

            // 提前根据配置表中的数据创建HBase的表
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

            // 修改广播状态中对应的业务表的数据的内容
            broadcastState.put(after.getSourceTable(), after);
        }

    }

    // 创建HBase中的维度表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        /**
         * 建表语句：
         *  create table if not exists 库名.表名 (id varchar primary key , name varchar) ext;
         */
        // 空值容错处理
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 默认主键处理
        if (sinkPk == null) {
            sinkPk = "id";
        }

        // 拼接建表语句
        StringBuilder sql = new StringBuilder("create table if not exists " + GmallConfig.PHOENIX_SCHEMA + "." + sinkTable + "(");
        String[] columns = sinkColumns.split(",");
        for (String column : columns) {
            if (column.equals(sinkPk)) {
                // 主键建表语句
                sql.append(column).append(" varchar primary key");
            } else {
                // 非主键建表语句
                sql.append(column).append(" varchar");
            }
            sql.append(",");
        }
        // 截取掉最后一个逗号，
        String result = sql.substring(0, sql.length() - 1) + ")" + sinkExtend;
        System.out.println("在Phoenix中执行的建表语句：" + result);

        // 创建HBase表

//        Connection connection = null;
//        PreparedStatement ps = null;
//        try {
//            // 使用Druid获取连接对象
//            connection = druidDataSource.getConnection();
//            // 创建执行对象
//            ps = connection.prepareStatement(result);
//            // 执行sql
//            ps.execute();
//            // 处理结果集
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("建表报错，sql:" + result);
//        } finally {
//            // 释放资源
//            if (ps != null) {
//                try {
//                    ps.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//            if (connection != null) {
//                try {
//                    connection.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
        // TODO 优化： 使用Druid连接池创建数据库连接和使用工具类执行SQL
        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
            PhoenixUtil.executeSQL(result, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
