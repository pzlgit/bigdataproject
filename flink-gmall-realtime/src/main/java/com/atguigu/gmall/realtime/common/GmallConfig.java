package com.atguigu.gmall.realtime.common;

/**
 * 定义全局配置信息
 *
 * @author pangzl
 * @create 2022-07-01 18:43
 */
public class GmallConfig {

    // 表命名空间
    public static final String PHOENIX_SCHEMA = "GMALL2022_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接地址
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

}

