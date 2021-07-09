package com.codeh.sql.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/**
 * @className Table04_Sink
 * @author jinhua.xu
 * @date 2021/5/7 17:14
 * @description Table中数据的输出
 * @version 1.0
 */
object Table04_Sink {
  def main(args: Array[String]): Unit = {
    // 获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 1. 读取文件中的数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt"

    // 1.1读取本地文件数据
    tableEnv.connect(new FileSystem().path(path)) // 表数据来源
      .withFormat(new Csv()) // 定义从外部系统读取数据之后的格式化方法
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ) // 对字段进行schema操作
      .createTemporaryTable("inputTable") // 创建的临时表名

    val sensorTable: Table = tableEnv.from("inputTable")

    // 2.1 简单转化
    //val resultTable: Table = sensorTable.select('id, 'temperature).filter('id === "sensor_01")

    // 2.2 聚合转化
    val aggTable: Table = sensorTable.groupBy('id).select('id, 'id.count as 'count)

    // 3.1 输出结果的文件路径
    val outPath: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\output.txt"

    tableEnv.connect(new FileSystem().path(outPath)) // 定义到文件系统的路径
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        //.field("temperature", DataTypes.DOUBLE())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")
// 可以将结果输出到kafka中
//    tableEnv.connect(new Kafka()
//      .version("0.11") // kafka版本
//      .topic("Sensor") // 定义主题
//      .property("zookeeper.connect", "192.168.214.136:2181")
//      .property("bootstrap.servers", "192.168.214.136:9092")
//    )
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("id", DataTypes.STRING())
//        .field("temperature", DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("kafkaOutput")

    //resultTable.insertInto("outputTable")
    //aggTable.insertInto("outputTable") 不能写入到文件，因为group by会造成count值变化，产生多条数据，该sink端不支持这个操作
    aggTable.toRetractStream[(String, Long)].print("aggregate")


    // 执行程序
    env.execute("out put file")
  }

}
