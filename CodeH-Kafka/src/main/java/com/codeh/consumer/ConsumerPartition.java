package com.codeh.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ConsumerPartition
 * @date 2021/4/1 16:57
 * @description 消费队列指定分区内的数据
 */
public class ConsumerPartition {
    public static void main(String[] args) {
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
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");

        // 创建消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        ArrayList<TopicPartition> partitions = new ArrayList<>();
        // 用来查询指定主题的元数据信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("topic-demo");
        if (partitionInfos != null) {
            for (PartitionInfo tpInfo : partitionInfos) {
                // 获取主题以及分区
                partitions.add(new TopicPartition(tpInfo.topic(), tpInfo.partition()));
            }
        }

        // 订阅全部分区的功能
        consumer.assign(partitions);

        // 消费消息
        try {
            while (true) {
                // 参数为超时时间
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record: records) {
                    System.out.println(record.key() + "---" + record.value());
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            consumer.close();
        }
    }
}
