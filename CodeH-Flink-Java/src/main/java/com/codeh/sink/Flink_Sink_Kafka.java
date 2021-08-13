package com.codeh.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Sink_Kafka
 * @date 2021/8/12 15:31
 * @description 使用Kafka作为输出端
 */
public class Flink_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("CodeH-Flink-Java/src/main/resources/word.txt");

        // kafka sink进行输出
        source.addSink(new FlinkKafkaProducer011<String>("192.168.214.136:9092", "sink", new SimpleStringSchema()));

        env.execute("Flink_Sink_Kafka");
    }
}
