package com.codeh.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] {"d:/input/wordcount","d:/output"};
        // 1.获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        // 指定需要使用combiner，以及用哪个类作为combiner的逻辑
        job.setCombinerClass(WordcountCombiner.class);

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
