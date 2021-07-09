package com.codeh.stream.window

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @className WaterMark
 * @author jinhua.xu
 * @date 2021/4/28 17:43
 * @description WaterMark是在我们处理的业务数据为乱序时，保证在一个特定的时间后，必须触发window去进行计算
 * @version 1.0
 */
object WaterMark {
  def main(args: Array[String]): Unit = {
    // 1.创建环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 引入EventTime的时间属性
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2.读取数据
    val ds: DataStream[String] = env.socketTextStream("192.168.214.136", 9999)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(3000)) { // 参数为最大乱序程度
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
    })

    // 4.窗口API的调用
    val windowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1) // 按照二元组的第一个元素进行分组（id）
      //      .window(TumblingEventTimeWindows.of(Time.seconds(15))) // 滚动窗口
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3))) // 滑动窗口
      .timeWindow(Time.seconds(10)) // 相当于窗口函数的简写
      .reduce((t1, t2) => (t1._1, t1._2.min(t2._2)))

    windowStream.print()


    env.execute("WaterMark")
  }
}
