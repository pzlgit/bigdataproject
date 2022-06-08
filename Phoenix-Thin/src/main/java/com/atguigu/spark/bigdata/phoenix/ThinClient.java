package com.atguigu.spark.bigdata.phoenix;

import org.apache.phoenix.queryserver.client.ThinClientUtil;

import java.sql.*;

/**
 * @author pangzl
 * @create 2022-06-01 20:34
 */
public class ThinClient {
    public static void main(String[] args) throws SQLException {
        // 1. 从瘦客户端获取链接
        String hadoop102 =
                ThinClientUtil.getConnectionUrl("hadoop102", 8765);
        // 2. 获取连接
        Connection connection =
                DriverManager.getConnection(hadoop102);
        // 3.查询语句
        PreparedStatement preparedStatement =
                connection.prepareStatement("select * from emp where id = ?");
        preparedStatement.setString(1, "1001");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            System.out.println(
                    resultSet.getString(1) + ":" +
                            resultSet.getString(2) + ":" +
                            resultSet.getString(3)
            );
        }
        preparedStatement.close();
        resultSet.close();
        connection.close();
    }
}
