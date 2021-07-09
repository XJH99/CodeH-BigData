package com.codeh.hotitem

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @className HotItemAnalysis_Kafka
 * @author jinhua.xu
 * @date 2021/5/8 14:37
 * @description 使用kafka作为数据源
 * @version 1.0
 */
object HotItemAnalysis_Kafka {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 使用事件事件进行处理
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", "192.168.214.136:9092")
    prop.setProperty("group.id", "consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    // 2.读取kafka数据并转化为样例类格式
    val kafkaStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotItems", new SimpleStringSchema(), prop))


    val userBehavior: DataStream[UserBehavior] = kafkaStream.map(
      data => {
        val datas: Array[String] = data.split(",")
        UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000) // 由于数据的时间是有序的，所以使用assignAscendingTimestamps，真实业务数据是乱序的

    // 3.过滤出点击数据
    val filterStream: DataStream[UserBehavior] = userBehavior.filter(_.behavior == "pv")

    // 4.在时间窗口内对商品进行聚合统计数量
    val resultStream: DataStream[String] = filterStream.keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5)) // 使用滑动窗口来实现功能
      .aggregate(new CountAgg(), new WindowResultFunction) // 做增量的聚合操作
      .keyBy("windowEnd") // 对窗口进行分组
      .process(new TopNHotItem(3)) // 得到点击量前三的产品

    // 5.打印输出
    resultStream.print("HotItems out")


    // 执行程序
    env.execute("Run HotItems")
  }
}
