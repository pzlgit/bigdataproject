package com.atguigu.gmall.realtime.util;

import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Phoenix 表查询方法
     *
     * @param conn 数据库连接对象
     * @param sql  查询数据的 SQL 语句
     * @param clz  返回的集合元素类型的class对象
     * @param <T>  返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();

            /**处理结果集
             +-----+----------+
             | ID  | TM_NAME  |
             +-----+----------+
             | 17  | lzls     |
             | 18  | mm       |
             class TM{id,tm_name}
             */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                resList.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发送异常了~~");
        } finally {
            //释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }
}
