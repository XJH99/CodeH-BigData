package com.codeh.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className MyInterceptor
 * @date 2021/3/31 10:16
 * @description 实现自定义拦截器功能，给发送的消息加个前缀，并统计消息发送的成功率
 */
public class MyInterceptor implements ProducerInterceptor {

    private Long success = 0L;
    private Long failure = 0L;


    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String value = "prefix-" + record.value();

        // 加上前缀返回
        return new ProducerRecord(record.topic(), record.partition(), record.timestamp(), record.key(), value, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            success ++;
        } else {
            failure ++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) success / (success + failure);
        System.out.println("发送消息的成功率为：" + (successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
