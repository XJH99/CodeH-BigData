package com.codeh.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className IOTest
 * @date 2021/3/29 15:44
 * @description 自定义文件上传下载
 */
public class IOTest {
    /**
     * 自定义文件上传
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.创建输入流
        FileInputStream fis = new FileInputStream(new File("e:/banhua.txt"));

        // 3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banhua.txt"));

        // 4.流对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5.关闭流资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 自定义文件下载
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/banhua.txt"));

        // 3.创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/banhua.txt"));

        // 4.流拷贝
        IOUtils.copyBytes(fis, fos, conf);

        // 5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 定位文件读取   读取第一块
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3.创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

        // 4.流的拷贝
        byte[] bytes = new byte[1024];

        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(bytes);
            fos.write(bytes);
        }

        // 5.关闭流资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 读取第二块
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
        // 1.获取文件系统
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        // 2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3.定位输入数据位置
        fis.seek(1024 * 1024 * 128);

        // 4.创建输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.2.tar.gz.part1"));

        // 5.流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 6.关闭流资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();

    }

}
