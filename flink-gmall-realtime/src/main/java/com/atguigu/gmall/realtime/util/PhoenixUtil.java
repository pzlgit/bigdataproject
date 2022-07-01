package com.atguigu.gmall.realtime.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Phoenix 工具类
 *
 * @author pangzl
 * @create 2022-07-01 19:46
 */
public class PhoenixUtil {

    // 执行DDL以及DML
    public static void executeSQL(String sql, Connection conn) {
        PreparedStatement ps = null;
        try {
            // 获取数据库操作对象
            ps = conn.prepareStatement(sql);
            // 执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("执行操作phoenix语句发生了异常");
        } finally {
            close(ps, conn);
        }
    }

    public static void close(PreparedStatement ps, Connection conn) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
