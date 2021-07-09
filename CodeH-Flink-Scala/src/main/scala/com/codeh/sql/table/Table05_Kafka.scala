package com.codeh.sql.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

/**
 * @className Table05_Kafka
 * @author jinhua.xu
 * @date 2021/5/7 17:41
 * @description kafka管道功能实现，kafka读入数据，对数据进行处理，通过kafka将数据写出
 * @version 1.0
 */
object Table05_Kafka {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2.读取kafka中的数据
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("Sensor")
      .property("zookeeper.connect", "192.168.214.136:2181")
      .property("bootstrap.servers", "192.168.214.136:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("kafkaInputTable")


    // 3.查询转换
    // 3.1 简单转化
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable: Table = sensorTable.select('id, 'temperature)
      .filter('id === "sensor_01")

    // 3.2 聚合转化
    //val aggTable: Table = sensorTable.groupBy('id).select('id, 'id.count as 'count)

    // 4.输出到kafka中
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("SinkTest")
      .property("zookeeper.connect", "192.168.214.136:2181")
      .property("bootstrap.servers", "192.168.214.136:9092")
    ).withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("kafkaOutputTable")

    // 写出数据
    resultTable.insertInto("kafkaOutputTable")

    // 5.执行
    env.execute("kafka")
  }

}
