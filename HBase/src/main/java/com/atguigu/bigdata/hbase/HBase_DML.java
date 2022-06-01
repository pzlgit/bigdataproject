package com.atguigu.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase 表操作
 *
 * @author pangzl
 * @create 2022-05-31 19:59
 */
public class HBase_DML {

    private static Connection connection = null;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        put("default", "student", "1008", "info", "salary", "100000");
//        get("default", "student", "1007", "info", "salary1");
//        scan("default", "student");
        delete("default", "student", "1001", "info", "name");
    }

    /**
     * 插入数据
     *
     * @param nameSpace    命名空间
     * @param tableName    表名称
     * @param columnFamily 列族
     * @param columnName   列
     * @param value        值
     */
    public static void put(String nameSpace,
                           String tableName,
                           String rowKey,
                           String columnFamily,
                           String columnName,
                           String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),
                Bytes.toBytes(value)
        );
        table.put(put);
        table.close();
    }

    /**
     * 查询数据
     *
     * @param nameSpace    命名空间
     * @param tableName    表名称
     * @param rowKey       键
     * @param columnFamily 列族
     * @param columnName   列
     */
    public static void get(String nameSpace, String tableName,
                           String rowKey, String columnFamily, String columnName
    ) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        // 指定列族和列查询
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneRow(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
        table.close();
    }

    /**
     * 扫描数据
     *
     * @param nameSpace 命名空间
     * @param tableName 表名称
     */
    public static void scan(String nameSpace, String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("1001"))
                .withStopRow(Bytes.toBytes("1001!"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        Bytes.toString(CellUtil.cloneRow(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                                Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        table.close();
    }

    /**
     * 删除表
     *
     * @param nameSpace 命名空间
     * @param tableName 表名称
     * @param rowKey    键
     * @param cf        cf
     * @param cn        cn
     */
    public static void delete(
            String nameSpace,
            String tableName,
            String rowKey,
            String cf,
            String cn
    ) throws IOException {
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 指定列族删除数据
//        delete.addFamily(Bytes.toBytes(cf));
//        // 指定列删除数据(所有版本)
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//        // 指定列删除数据(指定版本)
//        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        table.delete(delete);
        System.out.println("删除成功");

        table.close();
    }

}
