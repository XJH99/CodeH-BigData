package com.codeh.partition;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @date 2022/8/5 13:38
 * @describe kafka partition的使用
 * 分区的优点：
 * 1，便于合理使用存储资源，让数据均匀的发送到不同分区当中，做到负载均衡
 * 2，提高并行度，生产者可以以分区为单位发送数据，消费者可以以分区为单位消费数据
 */
public class PartitionDemo {
    public static void main(String[] args) throws InterruptedException {

        Logger logger = LoggerFactory.getLogger(PartitionDemo.class);

        Properties properties = new Properties();
        // 配置对象，broker服务器地址，key/value序列化对象
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            // 1，可以将数据发送到指定分区
            //producer.send(new ProducerRecord<String, String>("first-topic", "0", "kafka" + i), new Callback() {
            // 2，可以通过key的hash值与partition的个数进行取余得到分区值 a -> 0 dd -> 1
            producer.send(new ProducerRecord<String, String>("first-topic", "dd", "scala" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        // 说明没有发生异常，输出日志信息
                        logger.info("主题：{}, 分区：{}", metadata.topic(), metadata.partition());
                    } else {
                        // 出现异常情况
                        exception.printStackTrace();
                    }
                }
            });

            Thread.sleep(1000);
        }

        producer.close();
    }
}
