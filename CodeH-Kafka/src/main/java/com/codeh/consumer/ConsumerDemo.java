package com.codeh.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ConsumerDemo
 * @date 2021/3/30 15:17
 * @description 消费者示例代码
 */
public class ConsumerDemo {

    // 可以使用原子方式更新boolean值，保证在高并发的情况下只有一个线程能够访问这个属性值
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {

        Properties prop = new Properties();
        // 1.1 服务器配置
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.214.136:9092");
        // 1.2 反序列化
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 1.3 value反序列化
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 1.4 消费者组
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "come");
        // 1.5 客户端id,不设置会自动生成一个非空字符串，内容形式如“consumer-1”
        prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");

        // 创建消费者客户端实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);

        // 订阅主题
        //consumer.subscribe(Collections.singletonList("topic-demo"));
        //consumer.subscribe(Arrays.asList("topic-demo"));
        // 还可以使用正则表达式的方式进行订阅主题
        consumer.subscribe(Pattern.compile("topic-.*"));

        // 循环消费消息

        try {
            while (isRunning.get()) {
                // 参数为超时时间
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record: records) {
                    System.out.println(record.key() + "---" + record.value() + "---" + record.offset());
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
