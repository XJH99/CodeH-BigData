package com.codeh.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @className MarketByChannel
 * @author jinhua.xu
 * @date 2021/5/10 16:14
 * @description 不同渠道，行为分析数量的统计
 * @version 1.0
 */
object MarketByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1.读取自定义数据源的数据
    val stream: DataStream[MarketUserBehavior] = env.addSource(new MarketSource)
      .assignAscendingTimestamps(_.ts)

    // 2.数据处理
    val result: DataStream[MarketOutData] = stream.filter(_.behavior != "purchase")
      .keyBy(data => (data.behavior, data.channel))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel())

    result.print()

    env.execute("market by behavior and channel")
  }
}

/**
 * 数据的样例类
 */
case class MarketUserBehavior(userId: String, behavior: String, channel: String, ts: Long)

/**
 * 输出的数据样例
 *
 * @param windowStart
 * @param windowEnd
 * @param behavior
 * @param channel
 * @param count
 */
case class MarketOutData(windowStart: String, windowEnd: String, behavior: String, channel: String, count: Long)

/**
 * 继承RichSourceFunction，制造数据
 */
class MarketSource extends RichSourceFunction[MarketUserBehavior] {

  var running: Boolean = true

  override def cancel(): Unit = running = false


  val behaviorList: ListBuffer[String] = ListBuffer[String]("browse", "click", "view", "purchase", "install")
  val channelList: ListBuffer[String] = ListBuffer[String]("AppStore", "DY", "WEIBO", "WeChat", "tieBa")
  val rand: Random = Random

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L

    while (running
      && count < maxElements) {
      val userId = UUID.randomUUID().toString
      val behavior = behaviorList(rand.nextInt(behaviorList.size))
      val channel = channelList(rand.nextInt(channelList.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(userId, behavior, channel, ts))

      count += 1
      Thread.sleep(1000)
    }
  }
}


class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketOutData, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketOutData]): Unit = {
    val windowStart = new Timestamp(context.window.getStart).toString
    val windowEnd = new Timestamp(context.window.getEnd).toString
    val behavior = key._1
    val channel = key._2
    val count = elements.size

    out.collect(MarketOutData(windowStart, windowEnd, behavior, channel, count))
  }
}


