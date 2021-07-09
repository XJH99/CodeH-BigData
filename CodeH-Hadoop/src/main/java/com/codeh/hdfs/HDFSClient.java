package com.codeh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className HDFS API操作
 * @date 2021/3/29 15:30
 * @description TODO
 */
public class HDFSClient {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        // 1.创建配置对象
        Configuration conf = new Configuration();
        //conf.set("","");

        // 2.获取hdfs客户端对象
        //FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 3.在hdfs上创建路径
        fs.mkdirs(new Path("/user/0529"));

        // 4.关闭资源
        fs.close();

        System.out.printf("success");
    }


    // 文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        // 2.执行下载操作
        // 参数
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/banzhang.txt"), new Path("e:/banhua.txt"), true);
        // 3.关闭资源
        fs.close();
    }


    // 文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.执行上传操作
        /**
         * src: 本地文件路径
         * dst: 目标路径
         */
        fs.copyFromLocalFile(new Path("e:/banzhang.txt"), new Path("/banzhang.txt"));

        // 3.关闭资源
        fs.close();
    }

    // hdfs文件夹删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.执行删除操作
        // 第二个参数判断是否递归删除
        fs.delete(new Path("/0508/"), true);

        // 3.关闭资源
        fs.close();
    }

    // hdfs文件名进行更改
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.文件名更改
        /**
         * public abstract boolean rename(Path src, Path dst) throws IOException;
         * src：要修改的文件名
         * dist：修改后的文件名
         */
        fs.rename(new Path("/banzhang.txt"), new Path("/banhua.txt"));

        // 3.关闭资源
        fs.close();
    }

    // hdfs文件详情查看
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 3.迭代器遍历
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            // 输出详情数据
            System.out.printf(status.getPath().getName());  //文件名称
            System.out.printf(String.valueOf(status.getPermission()));  // 获取权限
            System.out.printf(String.valueOf(status.getLen())); //获取长度
            System.out.printf(status.getGroup());   //获取组信息

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation block : blockLocations) {
                // 获取主机存储节点
                String[] hosts = block.getHosts();
                for (String host : hosts) {
                    System.out.printf(host);
                }
            }

            System.out.printf("---------分割线-------------");
        }
    }

    // hdfs文件与文件夹判断
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.判断是文件还是文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        // 3.遍历处理
        for (FileStatus statue : fileStatuses) {
            if (statue.isFile()) {
                System.out.printf("f:" + statue.getPath().getName());
            } else {
                System.out.printf("d:" + statue.getPath().getName());
            }
        }
    }


}



