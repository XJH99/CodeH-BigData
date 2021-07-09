package com.codeh.sql.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}
/**
 * @className Table02_Source
 * @author jinhua.xu
 * @date 2021/5/6 11:28
 * @description 连接到文件系统，并读取数据
 * @version 1.0
 */
object Table02_Source {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 本地文件路径
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt";

    // 1.1读取本地文件数据
    tableEnv.connect(new FileSystem().path(path)) // 表数据来源
      .withFormat(new Csv()) // 定义从外部系统读取数据之后的格式化方法
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ) // 对字段进行schema操作
      .createTemporaryTable("inputTable") // 创建的临时表名

    // 1.2读取kafka中的数据
//    tableEnv.connect(new Kafka()
//      .version("0.11")  // kafka版本
//      .topic("Sensor")  // 定义主题
//      .property("zookeeper.connect", "192.168.214.136:2181")
//      .property("bootstrap.servers", "192.168.214.136:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("id", DataTypes.STRING())
//        .field("timestamp", DataTypes.BIGINT())
//        .field("temperature", DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("kafkaTable")

    // 2.读取表结构与数据
    val inputTable: Table = tableEnv.from("inputTable")

    // 3.table API操作
    // 3.1 API调用
    val apiRes: Table = inputTable
      .select("id, temperature")
      .filter("id = 'sensor_01'")

    // 3.2 table sql查询
    val sqlRes: Table = tableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id = 'sensor_01'
        |""".stripMargin)

    apiRes.toAppendStream[(String, Double)].print("api")
    sqlRes.toAppendStream[(String, Double)].print("sql")

    env.execute("table api sql test")

  }

}
