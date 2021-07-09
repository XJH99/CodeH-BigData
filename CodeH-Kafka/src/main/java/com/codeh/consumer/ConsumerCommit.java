package com.codeh.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ConsumerCommit
 * @date 2021/4/2 11:19
 * @description 验证lastConsumedOffset、committed offset和position之间的关系
 */
public class ConsumerCommit {
    public static void main(String[] args) {

        /**
         * position：下一次拉去的消息位置
         * lastConsumedOffset： 当前消费到的位置
         * committed offset：消费位移提交的位置
         *
         * position=committed offset=lastConsumedOffset+1
         *
         * consumer offset is 40
         * committed offset is 41
         * the offset of the next record is 41
         */

        Properties prop = new Properties();
        // 1.1 服务器配置
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.214.136:9092");
        // 1.2 反序列化
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 1.3 value反序列化
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // 1.4 消费者组
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "come");
        // 1.5 客户端id,不设置会自动生成一个非空字符串，内容形式如“consumer-1”
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo1");

        // 创建消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        TopicPartition partition = new TopicPartition("topic-demo", 0);

        // 订阅指定主题分区
        consumer.assign(Arrays.asList(partition));

        long lastConsumerOffset = -1;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            // 为空没有消息
            if (records.isEmpty()) {
                break;
            }

            // 指定订阅分区内的数据
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

            // 当前分区最后消费消息的偏移量
            lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

            // 同步提交消费位移
            consumer.commitSync();

        }

        System.out.println("consumer offset is " + lastConsumerOffset);

        // 获取offset的元数据信息
        OffsetAndMetadata committed = consumer.committed(partition);

        // 提交的位移
        System.out.println("committed offset is " + committed.offset());

        // 下次要拉取的起始偏移量
        long position = consumer.position(partition);

        System.out.println("the offset of the next record is " + position);

    }
}
