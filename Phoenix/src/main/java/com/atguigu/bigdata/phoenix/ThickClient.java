package com.atguigu.bigdata.phoenix;

import java.sql.*;
import java.util.Properties;

/**
 * @author pangzl
 * @create 2022-06-01 20:27
 */
public class ThickClient {

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled", "true");
        Connection connection = DriverManager.getConnection(url, properties);
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