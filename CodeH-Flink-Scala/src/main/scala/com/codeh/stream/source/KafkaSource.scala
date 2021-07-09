package com.codeh.stream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @className KafkaSource
 * @author jinhua.xu
 * @date 2021/4/28 16:17
 * @description 从kafka中读取数据
 * @version 1.0
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // kafka的配置信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "192.168.214.136:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 设置偏移量
    props.setProperty("auto.offset.reset", "latest")

    // 2.读取kafka中的数据
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("Sensor", new SimpleStringSchema(), props))

    // 3.打印数据
    stream.print()

    // 4.执行程序
    env.execute("Kafka Source")
  }

}
