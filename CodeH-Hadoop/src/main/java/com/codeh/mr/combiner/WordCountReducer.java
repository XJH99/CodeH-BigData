package com.codeh.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 继承Reducer类
 * 参数
 *  Text,IntWritable mapper的输出key value
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    int sum;
    IntWritable val = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;

        // 1.对传入的values进行求和
        for (IntWritable value: values) {
            sum += value.get();
        }

        // 2.将数据进行最终的输出
        val.set(sum);

        context.write(key,val);
    }
}
