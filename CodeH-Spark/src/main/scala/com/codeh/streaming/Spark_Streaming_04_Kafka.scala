package com.codeh.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @className Spark_Streaming_04_Kafka
 * @author jinhua.xu
 * @date 2021/4/25 10:55
 * @description Streaming 整合 Kafka
 * @version 1.0
 */
object Spark_Streaming_04_Kafka {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_04_Kafka").setMaster("local[*]")

    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    /**
     * 获取kafka消费端的参数
     */
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.214.136:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "Streaming_Kafka",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    /**
     * 消费kafka中的数据
     */
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      sc,
      LocationStrategies.PreferConsistent, // 在所有的executors分配分区
      ConsumerStrategies.Subscribe[String, String](Set("Streaming"), kafkaPara) // 订阅主题
    )

    // 打印value值
    kafkaData.map(_.value()).print()

    sc.start()
    sc.awaitTermination()
  }

}
