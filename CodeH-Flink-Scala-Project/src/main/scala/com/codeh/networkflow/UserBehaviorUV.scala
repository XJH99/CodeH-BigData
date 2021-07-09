package com.codeh.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @className UserBehaviorUV
 * @author jinhua.xu
 * @date 2021/5/10 11:16
 * @description 网站的UV数量统计分析
 * @version 1.0
 */
object UserBehaviorUV {
  def main(args: Array[String]): Unit = {
    // 获取环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\UserBehavior.csv"
    val fileStream: DataStream[String] = env.readTextFile(path)

    // 简单的转换操作
    val dataStream: DataStream[UserBehavior] = fileStream.map(
      data => {
        val lines: Array[String] = data.split(",")
        UserBehavior(lines(0).toLong, lines(1).toLong, lines(2).toLong, lines(3), lines(4).toLong)
      }
    ).assignAscendingTimestamps(_.ts * 1000L)

    val resultStream: DataStream[UVCount] = dataStream.filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UVWindow())

    resultStream.print()

    env.execute("UV job")
  }

}

// 输出结果样例类
case class UVCount(windowEnd: Long, count: Long)

// 窗口函数
class UVWindow extends AllWindowFunction[UserBehavior, UVCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {

    // 使用Set来去重
    var set: Set[Long] = Set()

    for(userBehavior <- input) {
      set += userBehavior.userId
    }

    out.collect(UVCount(window.getEnd, set.size))
  }
}
