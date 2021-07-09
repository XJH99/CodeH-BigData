package com.codeh.mr.partition;

import com.codeh.mr.serializable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区：按照手机号的开头进行分区
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 1.获取电话号码前三位
        String phone = text.toString().substring(0, 3);

        int partition = 4;
        if ("136".equals(phone)) {
            partition = 0;
        } else if ("137".equals(phone)) {
            partition = 1;
        } else if ("138".equals(phone)) {
            partition = 2;
        } else if ("139".equals(phone)) {
            partition = 3;
        }

        return partition;
    }
}
