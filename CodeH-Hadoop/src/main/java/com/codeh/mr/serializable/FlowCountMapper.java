package com.codeh.mr.serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, com.codeh.mr.serializable.FlowBean> {

    Text k = new Text();
    com.codeh.mr.serializable.FlowBean bean = new com.codeh.mr.serializable.FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // 1.获取每一行数据
        String str = value.toString();
        
        // 2.对数据进行分割
        String[] fields = str.split("\t");

        // 3.获取对应的值
        String phoneNum = fields[1];
        Long upFlow = Long.parseLong(fields[fields.length-3]);
        Long downFlow = Long.parseLong(fields[fields.length-2]);

        // 4.输出赋值
        k.set(phoneNum);
        bean.set(upFlow,downFlow);

        context.write(k,bean);

    }
}
