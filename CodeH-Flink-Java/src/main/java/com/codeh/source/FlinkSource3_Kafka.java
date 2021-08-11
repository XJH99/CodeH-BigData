package com.codeh.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FlinkSource3_Kafka
 * @date 2021/8/11 14:06
 * @description 从kafka中读取数据
 */
public class FlinkSource3_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka配置项
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.214.136:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // kafka中读取数据
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer011<String>("Sensor", new SimpleStringSchema(), properties));

        // 打印输出
        source.print();

        env.execute("FlinkSource3_Kafka");
    }
}
