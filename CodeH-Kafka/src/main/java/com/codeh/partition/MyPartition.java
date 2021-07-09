package com.codeh.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className MyPartition
 * @date 2021/3/30 21:15
 * @description 自定义分区器功能实现
 */
public class MyPartition implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        // 获取分区的个数
        int size = partitions.size();
        return key.toString().hashCode() % size;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
