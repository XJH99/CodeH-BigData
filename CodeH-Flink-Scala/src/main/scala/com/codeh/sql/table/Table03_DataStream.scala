package com.codeh.sql.table

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

/**
 * @className Table03_DataStream
 * @author jinhua.xu
 * @date 2021/5/6 14:36
 * @description 将dataStream转为Table
 * @version 1.0
 */
object Table03_DataStream {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt"
    val ds: DataStream[String] = env.readTextFile(path)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    // 从dataStream中转为Table
    val dataStreamTable: Table = tableEnv.fromDataStream(dataStream)

    val sensorTable2: Table = tableEnv.fromDataStream(dataStream, 'id, 'timestamp as 'ts)

    // 基于名称的对应
    val sensorTable3: Table = tableEnv.fromDataStream(dataStream, 'timestamp as 'ts, 'id as 'myId, 'temperature)

    // 基于位置的对应
    val sensorTable4: Table = tableEnv.fromDataStream(dataStream, 'myId, 'ts, 'temp)

  }

}
