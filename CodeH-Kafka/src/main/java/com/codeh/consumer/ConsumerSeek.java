package com.codeh.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ConsumerSeek
 * @date 2021/7/12 17:20
 * @description seek方法可以让消费者自定义消费位移
 */
public class ConsumerSeek {
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
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo1");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Arrays.asList("topic-demo"));

        Set<TopicPartition> assignment = new HashSet<>();

        while (assignment.size() == 0) { // 如果不为0，说明已经分配到分区
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        // 表示从指定的消费位移开始消费
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition, 10);
        }

        while (true) {
            ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : consumerRecords) {
                System.out.println(record.key() + "--" + record.value());
            }
        }
    }
}
