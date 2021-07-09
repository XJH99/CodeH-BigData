package com.codeh.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 继承mapper类
 * 参数：
 *  LongWritable：输入参数的键类型
 *  Text：输入参数的值类型
 *  Text：输出参数的键类型
 *  IntWritable：输出参数的值类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 全局对象，减少频繁的创建
    Text text = new Text();
    IntWritable val = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 数据 hadoop hadoop
        // 1.获取每一行的值
        String str = value.toString();

        // 2.对该行数据的空格进行分割
        String[] words = str.split(" ");

        // 3.遍历将数据输出
        for (String word:words) {
            text.set(word);
            // 将map结果写出，当作reduce的参数
            context.write(text,val);
        }
    }
}
