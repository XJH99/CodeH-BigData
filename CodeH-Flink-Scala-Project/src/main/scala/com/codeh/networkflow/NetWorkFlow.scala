package com.codeh.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @className NetWorkFlow
 * @author jinhua.xu
 * @date 2021/5/8 15:15
 * @description 基于服务器log的热门页面浏览器统计,统计一段时间内每个url访问用户的人数有多少，之后对结果排序输出
 * @version 1.0
 */
object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val fileStream: DataStream[String] = env.readTextFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\apache.log")

    val dataStream: DataStream[ApacheLog] = fileStream.map(
      data => {
        val lines: Array[String] = data.split(" ")
        val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = sdf.parse(lines(3)).getTime // 返回一个毫秒数的时间戳
        ApacheLog(lines(0), lines(2), timestamp, lines(5), lines(6))
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.seconds(1)) { // 乱序程度设置为1s左右
      override def extractTimestamp(element: ApacheLog): Long = element.eventTime // 注意这里的标准是要一个毫秒值
    }) // 乱序数据的处理


    // 开窗
    val aggStream: DataStream[LogCount] = dataStream.filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5)) // 窗口大小10分钟，滑动窗口5s
      .allowedLateness(Time.minutes(1)) // 允许的延迟时间为1分钟
      .sideOutputLateData(new OutputTag[ApacheLog]("late")) // 设置侧输出流
      .aggregate(new CountAggregate(), new WindowResultFunction())


    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))

    dataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[LogCount]("late")).print("late")
    resultStream.print()


    env.execute("NetWorkFlow")
  }

}

// 自定义累加器函数
class CountAggregate extends AggregateFunction[ApacheLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
 * Long: 输入累加数量的类型
 * LogCount：输出统计结果的类型
 * String： 分组key的类型
 * TimeWindow：时间窗口的类型
 */
class WindowResultFunction extends WindowFunction[Long, LogCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[LogCount]): Unit = {
    out.collect(LogCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrl(top: Int) extends KeyedProcessFunction[Long, LogCount, String] {

  //  lazy val countState: ListState[LogCount] = getRuntimeContext.getListState(new ListStateDescriptor[LogCount]("logCountState", classOf[LogCount]))

  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCountMapState", classOf[String], classOf[Long]))

  override def processElement(value: LogCount, ctx: KeyedProcessFunction[Long, LogCount, String]#Context, out: Collector[String]): Unit = {
    // countState.add(value)
    pageViewCountMapState.put(value.url, value.count)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    // 另外注册一个定时器，1分钟后触发，这时窗口已彻底关闭，不再有聚合结果输出，可以清空状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)

  }

  // 执行的方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LogCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    // 用于排序的ListBuffer
    //    val list: ListBuffer[LogCount] = ListBuffer()
    //
    //    val iter: util.Iterator[LogCount] = countState.get().iterator()
    //
    //    while (iter.hasNext) {
    //      list += iter.next()
    //    }
    //
    //    // 释放状态空间
    //    countState.clear()

    // 判断定时器触发时间，如果已经是窗口结束时间一分钟之后，那么直接清空状态
    if (timestamp == ctx.getCurrentKey + 60000L) {
      pageViewCountMapState.clear()
      return
    }

    val list: ListBuffer[(String, Long)] = ListBuffer()
    // 获取map迭代器数据
    val iter: util.Iterator[Map.Entry[String, Long]] = pageViewCountMapState.entries().iterator()

    while (iter.hasNext) {
      val entry: Map.Entry[String, Long] = iter.next()
      list += ((entry.getKey, entry.getValue))
    }

    //    val sortItems: ListBuffer[LogCount] = list.sortBy(_.count).reverse.take(top)
    val sortItems: ListBuffer[(String, Long)] = list.sortWith(_._2 > _._2).take(top)
    // 将排名信息格式化为String，便于打印数据
    val result: StringBuilder = new StringBuilder

    result.append("================================\n")
    result.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortItems.indices) {
      // 获取商品信息
      val currentItem = sortItems(i)

      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentItem._1)
        .append(" 统计量=").append(currentItem._2).append("\n")
    }

    result.append("================================\n\n")

    Thread.sleep(3000)

    out.collect(result.toString())

  }
}


// 定义日志数据的样例类
case class ApacheLog(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 输出结果的样例类
case class LogCount(url: String, windowEnd: Long, count: Long)

