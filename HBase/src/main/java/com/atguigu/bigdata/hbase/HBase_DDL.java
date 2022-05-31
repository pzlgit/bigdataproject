package com.atguigu.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 表DDL操作
 *
 * @author pangzl
 * @create 2022-05-31 18:15
 */
public class HBase_DDL {

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
        boolean tableExists = isTableExists("default", "student2");
        boolean table = createTable("default", "student2", "info1", "info2");
        dropTable("default", "student2");
        createNameSpace("test2");
        close();
    }

    /**
     * 判断表是否存在
     *
     * @param namespace 命名空间
     * @param tableName 表名称
     */
    public static boolean isTableExists(String namespace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(namespace, tableName));
        System.out.println(exists ? "表已存在！" : "表未存在");
        admin.close();
        return exists;
    }

    /**
     * 创建表
     *
     * @param namespace 命名空间
     * @param tableName 表名称
     * @param cfs       列族
     * @return flag
     * @throws IOException ex
     */
    public static boolean createTable(String namespace, String tableName, String... cfs) throws IOException {
        // 校验列族信息
        if (cfs.length <= 1) {
            System.out.println("请设置列族信息");
            return Boolean.FALSE;
        }
        // 校验表是否存在
        if (isTableExists(namespace, tableName)) {
            System.out.println("表已存在，无法创建！");
            return Boolean.FALSE;
        }

        Admin admin = connection.getAdmin();
        // 创建表描述器构造器
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));
        // 循环添加列族信息
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder familyBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            tableDescriptorBuilder.setColumnFamily(familyBuilder.build());
        }
        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("表创建成功！");
        admin.close();
        return Boolean.TRUE;
    }

    public static void dropTable(String nameSpace, String tableName) throws IOException {
        // 判断表是否存在
        if (!isTableExists(nameSpace, tableName)) {
            System.out.println("表不存在！");
            return;
        }
        Admin admin = connection.getAdmin();
        // 删除表需要先让表下线
        TableName name = TableName.valueOf(nameSpace, tableName);
        admin.disableTable(name);
        // 删除表
        admin.deleteTable(name);
        System.out.println("表删除成功！");
        admin.close();
    }

    /**
     * 创建命名空间
     *
     * @param nameSpace 命名空间
     * @throws IOException ex
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();
        try {
            admin.createNamespace(descriptor);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已存在！");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(nameSpace + "命名空间创建成功！");
        admin.close();
    }

    public static void close() throws IOException {
        connection.close();
    }

}
