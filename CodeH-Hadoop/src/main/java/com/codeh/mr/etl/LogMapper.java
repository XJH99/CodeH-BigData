package com.codeh.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 简单的数据清洗--
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.获取一行数据
        String line = value.toString();

        // 2.解析日志
        Boolean flag = parseLog(line,context);

        // 3.判断
        if (!flag) {
            return;
        }

        k.set(line);

        // 4.输出
        context.write(k, NullWritable.get());

    }

    // 日志解析方法
    private Boolean parseLog(String line, Context context) {
        // 1.切分
        String[] fields = line.split(" ");

        // 2.长度校验
        if (fields.length > 11) {
            context.getCounter("map","true").increment(1);
            return true;
        } else {
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
