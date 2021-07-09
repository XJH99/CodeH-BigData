package com.codeh.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @className Spark_Streaming_10_MockData
 * @author jinhua.xu
 * @date 2021/4/29 10:42
 * @description 生成模拟数据的功能
 * @version 1.0
 */
object Spark_Streaming_10_MockData {
  def main(args: Array[String]): Unit = {
    // 将生成的数据导入到kafka中 Application =》 kafka =》SparkStream =》Analysis

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.214.136:9092")
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 1.创建kafka生产者实例
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    // 2.遍历模拟数据
    while (true) {
      mockData.foreach(
        data => {
          // 封装数据到record中
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("spark_streaming_kafka", data)

          // 异步发送消息，指定Callback的回调函数
          try {
            producer.send(record, new Callback {
              override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
                if (e != null) {
                  e.printStackTrace()
                } else {
                  println(recordMetadata.topic() + "---" + recordMetadata.offset() + "---" + data)
                }
              }
            })
          } catch {
            case ex: Exception => ex.printStackTrace()
          }
        }
      )

      Thread.sleep(2000)
    }
  }


  /**
   * 生成模拟数据的方法
   *
   * @return mockData: ListBuffer[String]
   */
  def mockData: ListBuffer[String] = {
    val mockDataList = ListBuffer[String]()
    val cityList = ListBuffer[String]("北京", "上海", "浙江")
    val areaList = ListBuffer[String]("华东", "华南", "华北")

    /**
     * 制造随机的50条数据
     */
    for (i <- 1 to 50) {
      // 城市
      val city: String = cityList(new Random().nextInt(3))
      // 区域
      val area: String = areaList(new Random().nextInt(3))
      // 用户id
      val userId: Int = new Random().nextInt(6) + 1
      // 广告id
      val adid: Int = new Random().nextInt(6) + 1

      mockDataList.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adid}")
    }

    mockDataList
  }
}
