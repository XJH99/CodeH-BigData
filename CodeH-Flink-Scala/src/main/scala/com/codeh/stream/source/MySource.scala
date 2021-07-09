package com.codeh.stream.source

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.collection.immutable

/**
 * @className MySource
 * @author jinhua.xu
 * @date 2021/4/28 16:25
 * @description 自定义Source功能实现
 * @version 1.0
 */
object MySource {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 2.读取自定义数据源的数据
    val stream: DataStream[SensorReading] = env.addSource(new MySensorSource)

    // 3.打印数据
    stream.print()

    // 4.执行程序
    env.execute("MySource")

  }
}

/**
 * 自定义数据源功能实现
 */
class MySensorSource extends SourceFunction[SensorReading] {

  // 用于设置程序状态的全局变量
  var running: Boolean = true

  /**
   * 实际生成数据的方法
   *
   * @param ctx 用于发送数据
   */
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val random: Random = new Random()

    // 制造一个元组数据
    var tuples: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("Sensor_" + i, random.nextDouble() * 100))

    while (true) {
      // nextGaussian() 方法产生的数据类似于正态分布的数值
      tuples = tuples.map(data => (data._1, data._2 + random.nextGaussian()))

      // 2.获取当前系统的时间戳
      val current: Long = System.currentTimeMillis()

      /**
       * 3.对产生的基础数据进行转化处理,并发送
       *
       * ctx.collect() 发送数据的方法
       */
      tuples.foreach(
        data => ctx.collect(SensorReading(data._1, current, data._2))
      )

      // 4.休眠1秒钟
      Thread.sleep(1000)
    }
  }

  /**
   * 用于关闭 Source 程序
   */
  override def cancel(): Unit = running = false
}
