package com.codeh.stream.process

import com.codeh.stream.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @className ProcessFunctionDemo
 * @author jinhua.xu
 * @date 2021/4/28 17:55
 * @description ProcessFunction是用来构建事件驱动的应用以及实现自定义的业务逻辑
 * @version 1.0
 */
object ProcessFunctionDemo {
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

    val warningStream = dataStream.keyBy(_.id).process(new TempIncreWarning(10000L))

    warningStream.print()

    env.execute("process function test")
  }
}

/**
 * 实现自定义的KeyedProcessFunction
 * @param interval
 * KeyedProcessFunction[String, SensorReading, String]:第一个参数表示上一个分组key的类型
 */
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {


  // 定义状态，保存上一个温度值进行比较
  lazy val lastTemperatureState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTempState", classOf[Double]))
  // 保存注册的定时器的时间戳
  lazy val lastTimeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTimeState", classOf[Long]))

  /**
   * 监控温度传感器的温度值，当温度值在一秒之内连续上升，则报警
   * @param input
   * @param ctx
   * @param out
   */
  override def processElement(input: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

    // 取出上一次的温度
    val lastTemp = lastTemperatureState.value()
    // 取出定时器的时间戳
    val lastTime = lastTimeState.value()

    // 更新最新传入的温度值
    lastTemperatureState.update(input.temperature)

    // 当温度上升时,定时器没有时
    if (input.temperature > lastTemp && lastTime == 0L) {
      // 温度上升，并且没有定时器，那么注册当前时间10s之后的定时器
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      lastTimeState.update(ts)
    } else if (input.temperature < lastTemp) {
      // 移除定时器
      ctx.timerService().deleteProcessingTimeTimer(lastTime)
      // 清空时间状态
      lastTimeState.clear()
    }
  }

  // 回调函数，输出结果的集合
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器id为：" + ctx.getCurrentKey + "传感器的温度值已连续上升了"+ interval / 1000 +"秒")
    // 清空时间状态值
    lastTimeState.clear()
  }
}

