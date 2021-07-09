package com.codeh.mr.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReduce extends Reducer<Text,FlowBean, Text,FlowBean> {

    FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sumUpFlow = 0;
        long sumDownFlow = 0;
        // 1.获取相同号码values值
        for (FlowBean value:values) {
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }

        // 2.封装对象
        flowBean.set(sumUpFlow,sumDownFlow);

        // 3.输出结果
        context.write(key,flowBean);
    }
}
