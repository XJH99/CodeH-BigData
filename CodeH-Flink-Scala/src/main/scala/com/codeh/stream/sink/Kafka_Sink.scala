package com.codeh.stream.sink

import java.util.Properties

import com.codeh.stream.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @className Kafka_Sink
 * @author jinhua.xu
 * @date 2021/4/28 16:58
 * @description 使用Kafka作为输出源
 * @version 1.0
 */
object Kafka_Sink {
  def main(args: Array[String]): Unit = {
    // 1.创建环境对象
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // kafka的配置信息
    val props = new Properties()
    props.setProperty("bootstrap.servers", "192.168.214.136:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 设置偏移量
    props.setProperty("auto.offset.reset", "latest")

    // 从kafka读取数据
    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("Sensor", new SimpleStringSchema(), props))

    // 数据转化处理
    val dataStream: DataStream[String] = kafkaStream.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble).toString
    })

    // 将数据写出到kafka中
    dataStream.addSink(new FlinkKafkaProducer011[String]("192.168.214.136:9092", "sink", new SimpleStringSchema()))

    // 执行程序
    env.execute("kafka_sink")

  }
}
