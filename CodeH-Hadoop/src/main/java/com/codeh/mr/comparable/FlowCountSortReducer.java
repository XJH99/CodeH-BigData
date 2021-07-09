package com.codeh.mr.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 循环输出，避免总流量相同情况
        for (Text text : values) {
            context.write(text, key);
        }

    }
}
