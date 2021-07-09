package com.codeh.sql.window

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @className TimeWindow
 * @author jinhua.xu
 * @date 2021/5/7 17:58
 * @description 时间特性,处理时间ProcessingTime的使用
 * @version 1.0
 */
object TimeWindow {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    /**
     * DataStream 转化成 Table 时指定
     */
    // 2.读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt"
    val ds: DataStream[String] = env.readTextFile(path)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    // 从流中读取数据转为table
    val proceTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'pt.proctime)

    // 打印对应的表结构
    proceTable.printSchema()

    proceTable.toAppendStream[Row].print()


    /**
     * 定义 Table Schema 时指定
     */
    //    val path: String = "D:\\IDEA_Project\\flink_scala\\src\\main\\resources\\sensor.txt"
    //
    //    tableEnv.connect(new FileSystem().path(path))
    //        .withFormat(new Csv())
    //        .withSchema(
    //          new Schema()
    //            .field("id", DataTypes.STRING())
    //            .field("timestamp", DataTypes.BIGINT())
    //            .field("temperature", DataTypes.DOUBLE())
    //            .field("pt", DataTypes.TIMESTAMP(3))
    //            .proctime() // 指定 pt 字段为处理时间
    //        ).createTemporaryTable("inputFile")
    //
    //    val inputFileTable: Table = tableEnv.from("inputFile")
    //
    //    val resultTable: Table = inputFileTable.select('id, 'timestamp, 'temperature, 'pt)
    //
    //    //inputFileTable.printSchema()
    //
    //    resultTable.toAppendStream[Row].print("Schema")


    env.execute("time test1")
  }

}
