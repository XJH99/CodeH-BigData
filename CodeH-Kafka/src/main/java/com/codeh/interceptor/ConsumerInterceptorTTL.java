package com.codeh.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ConsumerInterceptor
 * @date 2021/7/15 14:48
 * @description 消费者拦截器, 用于判断消费数据是否过期
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {

    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long currentTimeMillis = System.currentTimeMillis();
        HashMap<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();

        for (TopicPartition tp : records.partitions()) {
            // 获取某个分区内的所有消息
            List<ConsumerRecord<String, String>> tpRecords = records.records(tp);

            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();

            // 对获取的所有消息遍历
            for (ConsumerRecord<String, String> record : tpRecords) {
                // 当前时间 - 消息的时间 < EXPIRE_INTERVAL
                if (currentTimeMillis - record.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(record);
                }
            }

            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach(((topicPartition, offsetAndMetadata) -> System.out.println(topicPartition + ":" + offsetAndMetadata.offset())));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
