package com.codeh.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 对每一个MapTask的输出进行局部汇总，以减小网络传输量即采用Combiner功能
 */
public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value:values) {
            sum += value.get();
        }

        v.set(sum);

        context.write(key,v);
    }
}
