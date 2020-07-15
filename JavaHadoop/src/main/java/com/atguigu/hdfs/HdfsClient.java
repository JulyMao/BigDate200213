package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.io.IOException;
import java.net.URI;

/**
 * @author maow
 * @create 2020-04-13 19:27
 */
public class HdfsClient {
    FileSystem fileSystem;

    @Before
    public void fileCreate() throws IOException, InterruptedException {
        URI uri = URI.create("hdfs://hadoop102:9820");
        Configuration entries = new Configuration();
        String user = "atguigu";
        fileSystem = FileSystem.get(uri, entries, user);
    }

    @After
    public void fileStop() throws IOException {
        fileSystem.close();
    }

    @Test
    public void testHdfsClient() throws IOException, InterruptedException {
        //1. 创建HDFS客户端对象,传入uri， configuration , user
        URI uri = URI.create("hdfs://hadoop102:9820");
        Configuration entries = new Configuration();
        String user = "atguigu";
        FileSystem fileSystem = FileSystem.get(uri, entries, user);
        //2.操作集群
        fileSystem.mkdirs(new Path("/JavaTest"));
        //3.关闭资源
        fileSystem.close();
    }

    @Test
    public void upLoadFile() throws IOException, InterruptedException {
        //1.创建HDFS客户端对象，传入uri,configuration,user
        URI uri = URI.create("hdfs://hadoop102:9820");
        Configuration entries = new Configuration();
        String user = "atguigu";
        FileSystem fileSystem = FileSystem.get(uri, entries, user);
        //2.操作集群
        fileSystem.copyFromLocalFile(new Path("D:/课堂笔记4.txt"), new Path("/JavaTest"));
        //3.关闭资源
        fileSystem.close();
    }

    @Test
    public void downLoad() throws IOException {
        fileSystem.copyToLocalFile(false,new Path("/JavaTest"), new Path("D:/"),true);
    }

    @Test
    public void delFile() throws IOException {
        fileSystem.delete(new Path("/JavaTest"), true);
    }

    @Test
    public void file() throws IOException {
        fileSystem.copyFromLocalFile(new Path("D:/课堂笔记4.txt"),new Path("/"));
    }

    @Test
    public void renameFile() throws IOException {
//        fileSystem.rename(new Path("/课堂笔记4.txt"), new Path("/课堂笔记!!!.txt"));
        fileSystem.rename(new Path("/课堂笔记!!!.txt"), new Path("/JavaTest/课堂笔记4.txt"));
    }

    @Test
    public void listFile() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath());
            System.out.println("============================");
            System.out.println(next.getPath().getName());
            System.out.println("============================");
            System.out.println(next.getLen());
            System.out.println("============================");
            System.out.println(next.getPermission());
            System.out.println("============================");
            System.out.println(next.getGroup());
            System.out.println("============================");
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation block : blockLocations) {
                System.out.println(block.toString());
            }
        }
    }

}
