package com.codeh.streaming

import java.text.SimpleDateFormat

import com.codeh.utils.JDBCUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @className Spark_Streaming_12_Request
 * @author jinhua.xu
 * @date 2021/4/29 14:44
 * @description 广告点击量实时统计：实时统计每天各地区各城市各广告的点击总流量，并将其存入 MySQL
 * @version 1.0
 */
object Spark_Streaming_12_Request {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Streaming_12_Request")

    // 第二个参数表示批量处理的周期
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 定义kafka的一些配置
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.214.136:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "hypers",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )


    // 1.kafka工具类获取消费者订阅的数据
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("spark_streaming_kafka"), kafkaPara)
    )

    // 2.将kafka中读取的数据封装成样例类模式
    val adClkData: DStream[AdClkData] = kafkaData.map(
      data => {
        val values: Array[String] = data.value().split(" ")
        AdClkData(values(0), values(1), values(2), values(3), values(4))
      }
    )

    // 3.聚合操作
    val ds: DStream[((String, String, String, String), Int)] = adClkData.map(
      data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new java.util.Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad

        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    ds.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          item => {
            val conn = JDBCUtils.getConnection
            val state = conn.prepareStatement(
              """
                |insert into area_city_ad_count ( dt, area, city, adid, count )
                | values ( ?, ?, ?, ?, ? )
                | on DUPLICATE KEY
                | UPDATE count = count + ?
                |""".stripMargin)
            item.foreach {
              case ((day, area, city, ad), sum) => {
                state.setString(1, day)
                state.setString(2, area)
                state.setString(3, city)
                state.setString(4, ad)
                state.setInt(5, sum)
                state.setInt(6, sum)
                state.executeUpdate()
              }
            }
            state.close()
            conn.close()
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()

  }

}
