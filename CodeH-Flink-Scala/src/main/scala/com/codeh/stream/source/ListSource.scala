package com.codeh.stream.source

import org.apache.flink.streaming.api.scala._

/**
 * @className ListSource
 * @author jinhua.xu
 * @date 2021/4/28 15:50
 * @description 从集合中读取数据
 * @version 1.0
 */
object ListSource {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 2.创建集合
    val dataList = List(
      SensorReading("sensor_01", 1000000001, 39.5),
      SensorReading("sensor_03", 1000000002, 33.5),
      SensorReading("sensor_05", 1000000003, 30.5),
      SensorReading("sensor_07", 1000000004, 37.5)
    )

    // 3.读取集合中数据
    val stream: DataStream[SensorReading] = env.fromCollection(dataList)

    // 4.打印数据
    stream.print()

    // 5.执行程序
    env.execute("List Source")
  }
}

// 温度样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)
