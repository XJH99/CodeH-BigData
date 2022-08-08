package com.codeh.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @date 2022/8/4 23:22
 * @describe 生产者发送数据
 */
public class CustomProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Logger logger = LoggerFactory.getLogger(CustomProducer.class);
        Properties properties = new Properties();
        // 配置对象，broker服务器地址，key/value序列化对象
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 生产者提高吞吐量的优化参数
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);  //批次大小，默认16k
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // 消息缓冲区大小，默认32M，优化可以调整到64M
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 等待时间，默认为0
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 数据压缩方式
        properties.put(ProducerConfig.ACKS_CONFIG, "all"); // 应答机制，有 0，1，-1三种情况
        properties.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数，默认是int的最大值
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 1.创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 2.异步发送数据
//    for (int i = 0; i < 5; i++) {
//      producer.send(new ProducerRecord<>("first-topic", "hello" + i));
//    }

        // 3.同步发送数据，相对于异步发送是调用send方法后再调用get方法就可以
//    for (int i = 0; i <5; i++) {
//      producer.send(new ProducerRecord<>("first-topic", "Synchronize" + i)).get();
//    }


        // 4.带回调函数的异步发送
        for (int i = 0; i < 5; i++) {

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first-topic", "come" + i);

            producer.send(producerRecord, new Callback() {
                // 回调函数，该方法会在Producer收到ack时调用，为异步调用
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

            // 延迟一会看数据发往不同的分区，可以发现数据会均匀的发送到不同分区起到负载均衡的作用
            Thread.sleep(1000);
        }

        // 3.关闭资源
        producer.close();
    }
}
