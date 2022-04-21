package com.atguigu.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * HDFS API 操作
 *
 * @author pangzl
 * @create 2022-04-21 11:38
 */
public class HdfsClient {

    private URI uri;
    private Configuration configuration;
    private FileSystem system;
    private String user;

    @Before
    public void init() throws Exception {
        uri = new URI("hdfs://hadoop102:9820");
        configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        user = "atguigu";
        system = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() throws Exception {
        system.close();
    }

    @Test
    public void testMkdir() throws Exception {
        boolean mkdirs = system.mkdirs(new Path("/pzl"));
        System.out.println(mkdirs);
    }

    /**
     * 文件上传
     *
     * @throws Exception
     */
    @Test
    public void testPut() throws Exception {
        system.copyFromLocalFile(
                false,
                true,
                new Path("D:\\WorkShop\\BIgDataProject\\Data\\word.txt"),
                new Path("/java")
        );
    }

    @Test
    public void testGet() throws Exception {
        system.copyToLocalFile(true,
                new Path("/java/word.txt"),
                new Path("D:\\WorkShop\\BIgDataProject\\Data"),
                true
        );
    }

    @Test
    public void testRm() throws Exception {
        // 删除目标文件
//        system.delete(new Path("/java/word.txt"), false);
        // 删除空目录
        //system.delete(new Path("/java"), false);
        // 删除非空目录
        system.delete(new Path("/java"), true);
    }

    @Test
    public void testMv() throws Exception {
        // 文件更名
        //system.rename(new Path("/java/word.txt"),new Path("/java/1.txt"));
        // 文件移动并且更名
        //system.rename(new Path("/java/1.txt"),new Path("/pzl/999.txt"));
        // 目录更名
        // system.rename(new Path("/java"),new Path("/java1"));
        system.rename(new Path("/java1"), new Path("/pzl"));
    }


    @Test
    public void testListFiles() throws Exception {
        // 文件详情查看
        RemoteIterator<LocatedFileStatus> iterator = system.listFiles(new Path("/"), true);
        // 遍历查看信息
        while (iterator.hasNext()) {
            LocatedFileStatus status = iterator.next();
            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
    }

    @Test
    public void testFileOrDir() throws Exception {
        // HDFS文件和文件夹判断
        FileStatus[] fileStatuses = system.listStatus(new Path("/pzl"));
        for (FileStatus fileStatus : fileStatuses) {
            boolean file = fileStatus.isFile();
            if (file) {
                System.out.println("文件：" + fileStatus.getPath());
            } else {
                System.out.println("目录：" + fileStatus.getPath());
            }
        }
    }

    @Test
    public void testIsAll() throws Exception {
        isAll("/pzl", system);
    }

    public void isAll(String path, FileSystem system) throws Exception {
        // HDFS递归判断传入路径下的文件和目录
        FileStatus[] fileStatuses = system.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            boolean file = fileStatus.isFile();
            if (file) {
                System.out.println("文件：" + fileStatus.getPath());
            } else {
                System.out.println("目录：" + fileStatus.getPath());
                isAll(fileStatus.getGroup().toString(), system);
            }
        }


    }

    @Test
    public void testPutByIO() throws Exception {
        // 获取本地文件输入流
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\WorkShop\\BIgDataProject\\Data\\word.txt"));
        // 获取hdfs文件输出流
        FSDataOutputStream fsDataOutputStream = system.create(new Path("/pzl/aaa.txt"));
        // 流的对拷
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);
        // 流的关闭
        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(fileInputStream);
    }

    @Test
    public void testGetByIO() throws Exception {
        // 获取hdfs文件输入流
        FSDataInputStream open = system.open(new Path("/pzl/999.txt"));
        // 获取本地文件输出流new Path("")
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\\\WorkShop\\\\BIgDataProject\\\\Data\\\\word1.txt"));
        // 流的对拷
        IOUtils.copyBytes(open,fileOutputStream,configuration);
        // 流的关闭
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(open);
    }

}