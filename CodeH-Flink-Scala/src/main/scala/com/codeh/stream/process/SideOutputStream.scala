package com.codeh.stream.process

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @className SideOutputStream
 * @author jinhua.xu
 * @date 2021/4/29 18:46
 * @description 侧输出流功能实现
 * @version 1.0
 */
object SideOutputStream {
  def main(args: Array[String]): Unit = {
    // 1.创建环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val ds: DataStream[String] = env.socketTextStream("192.168.214.136", 9999)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    val monitoredReadings: DataStream[SensorReading] = dataStream.process(new FreezingMonitor)

    // 4.获取侧输出流
    monitoredReadings.getSideOutput(new OutputTag[String]("freezing-alarms")).print("侧输出流")

    dataStream.print("主流")

    env.execute("SideOutputStream")

  }
}

/**
 * 自定义侧输出流实现：将温度低于32度的值输出到侧输出流
 */
class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading]{

  lazy val freezingAlarmOutput: OutputTag[String] = new OutputTag[String]("freezing-alarms")

  override def processElement(input: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 温度低于32度，侧输出流输出
    if (input.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${input.id}")
    } else {
      // 主流输出
      out.collect(input)
    }
  }
}
