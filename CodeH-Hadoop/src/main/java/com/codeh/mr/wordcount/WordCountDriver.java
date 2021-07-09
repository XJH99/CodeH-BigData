package com.codeh.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {"D:\\IDEA_Project\\CodeH-BigData\\CodeH-Hadoop\\data\\input\\word",
                "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Hadoop\\data\\output"};
        // 1.获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);


        // 设置驱动类
        //job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置为20m
        //CombineTextInputFormat.setMaxInputSplitSize(job,20971520);

        // 2.设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        // 3.关联map与reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置mapper阶段输出数据的key与value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5.设置最终数据输出的key与value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6.设置输入路径与输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7.提交job
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);

    }
}
