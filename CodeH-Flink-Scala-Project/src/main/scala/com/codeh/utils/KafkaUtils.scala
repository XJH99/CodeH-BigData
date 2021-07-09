package com.codeh.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
 * @className KafkaUtils
 * @author jinhua.xu
 * @date 2021/5/8 14:42
 * @description kafka生产者工具类，用于读取本地数据发送到kafka中
 * @version 1.0
 */
object KafkaUtils {
  def main(args: Array[String]): Unit = {
    sendDataKafka("hotItems")
  }

  // 生产者读取文件数据，写入到kafka中
  def sendDataKafka(topic: String): Unit = {
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", "192.168.214.136:9092")
    // key/value要进行序列化操作
    prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 读取文件中的数据
    val source: BufferedSource = io.Source.fromFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\UserBehavior.csv")

    // 创建producer对象
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    // 遍历获取每一行数据
    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)

      // 发送数据
      producer.send(record)
    }

    // 关闭资源
    producer.close()
  }

}
