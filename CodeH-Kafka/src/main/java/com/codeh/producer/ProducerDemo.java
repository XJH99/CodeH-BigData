package com.codeh.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ProducerDemo
 * @date 2021/3/30 15:05
 * @description 生产者示例代码
 */
public class ProducerDemo {
    public static void main(String[] args) {
        Properties prop = new Properties();
        // 1.1 服务器配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.214.136:9092");

        // 1.2 ack回复机制
        prop.put(ProducerConfig.ACKS_CONFIG, "all");

        // 1.3 重试次数
        prop.put(ProducerConfig.RETRIES_CONFIG, 1);

        // 1.4 批次大小
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 1.5 等待时间
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // 1.6 RecordAccumulator缓冲区大小,默认是下面的32M
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // 1.7 key/value序列化
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        // 拦截器
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hypers.interceptor.MyInterceptor");

        // 创建KafkaProducer实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        for (int i = 0; i <5 ; i++) {
            // 构建需要发送的消息
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-demo", "hello, kafka-" + i);


            try {
                // 正常发送消息
                //producer.send(record);

                // 同步发送可以利用返回的Future对象实现,可以阻塞等待kafka的响应，直到消息发送成功或发生异常
                //producer.send(record).get();

                // 异步发送消息，指定Callback的回调函数
                producer.send(record, new Callback() {
                    // 回调函数，该方法会在Producer收到ack时调用，为异步调用
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.println(metadata.topic() + "---" + metadata.offset());
                        }
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 关闭客户端
        producer.close();


    }
}
