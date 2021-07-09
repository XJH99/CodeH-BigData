package com.codeh.market

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @className AdidClickAnalysis
 * @author jinhua.xu
 * @date 2021/5/10 18:09
 * @description 广告页面点击分析
 * @version 1.0
 */
object AdClickAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val adStream: DataStream[String] = env.readTextFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\AdClickLog.csv")

    val mapStream: DataStream[AdClickLog] = adStream.map(data => {
      val dataArray = data.split(",")
      AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
    })
      .assignAscendingTimestamps(_.ts * 1000L)

    mapStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new CountAdAgg(), new CountResult()).print()

    env.execute("ad click job")

  }
}

case class AdClickLog(userId: Long, adId: Long, province: String, city: String, ts: Long)

case class CountByProvince(windowEnd: String, province: String, count: Long)

/**
 * 累加器
 */
class CountAdAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class CountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.last))
  }
}
