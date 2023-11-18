package com.codeh.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @date 2022/8/9 16:43
 * @describe kafka消费者用于测试同一个消费者组中的消费者会消费不同的分区数据
 */
public class CustomConsumerBak {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 添加kafka服务器参数，key/value序列化参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 配置消费者组，名称可以任意取
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "first");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 消费者订阅主题
        consumer.subscribe(Collections.singletonList("first-topic"));

        // 消费某个主题某个分区的数据,下面的代码只能消费0号分区的数据
//        ArrayList<TopicPartition> list = new ArrayList<>();
//        list.add(new TopicPartition("first-topic", 0));
//        consumer.assign(list);

        while (true) {
            // 1s内消费一批数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));

            // 打印消费到的消息
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record);
            }
        }
    }
}
