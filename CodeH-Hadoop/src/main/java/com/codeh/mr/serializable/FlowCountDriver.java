package com.codeh.mr.serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"d:/input/flowcount","d:/output"};
        // 1.获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.加载jar所在路径
        job.setJarByClass(com.codeh.mr.serializable.FlowCountDriver.class);

        // 3.关联mapper与reduce类
        job.setMapperClass(com.codeh.mr.serializable.FlowCountMapper.class);
        job.setReducerClass(com.codeh.mr.serializable.FlowCountReduce.class);

        // 4.获取mapper输出的key/value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(com.codeh.mr.serializable.FlowBean.class);

        // 5.获取输出结果的key/value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(com.codeh.mr.serializable.FlowBean.class);

        // 添加驱动，默认是TextInputFormat.class
        //job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置虚拟存储切片最大值为20m
        //CombineTextInputFormat.setMaxInputSplitSize(job,20971520);

        // 6.指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7.提交任务
        boolean result = job.waitForCompletion(true);

        System.exit(result? 0:1);
    }
}
