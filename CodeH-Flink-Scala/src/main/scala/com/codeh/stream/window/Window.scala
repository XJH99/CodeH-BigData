package com.codeh.stream.window

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @className Window
 * @author jinhua.xu
 * @date 2021/4/28 17:27
 * @description window是一种切割无限数据为有限块进行处理的手段
 * @version 1.0
 */
object Window {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 引入EventTime的时间属性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val ds: DataStream[String] = env.socketTextStream("192.168.214.136", 9999)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    val windowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy("id")// 按照二元组的第一个元素进行分组（id）
      //      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // 滚动窗口
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3))) // 滑动窗口
      //.timeWindow(Time.seconds(10)) // 相当于滚动窗口的简写
      .timeWindow(Time.seconds(10), Time.seconds(5)) // 窗口大小为10s，滑动步长为5s
      .reduce((t1, t2) => (t1._1, t1._2.min(t2._2)))

    // 打印数据
    windowStream.print()

    // 执行程序
    env.execute("Window Demo")

  }
}
