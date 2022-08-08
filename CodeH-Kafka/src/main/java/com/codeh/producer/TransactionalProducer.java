package com.codeh.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @date 2022/8/5 15:44
 * @describe 事务保证producer发送消息的精准一次性
 */
public class TransactionalProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 配置对象，broker服务器地址，key/value序列化对象
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置事务ID，事务ID可以任意起名
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_01");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 初始化事务并开启事务
        producer.initTransactions();
        producer.beginTransaction();

        try {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("first-topic", "tran" + i));
            }
            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            // 终止事务
            producer.abortTransaction();
        } finally {
            // 关闭资源
            producer.close();
        }
    }
}
