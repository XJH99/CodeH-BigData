package com.codeh.networkflow

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.table.runtime.operators.window.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * @className UserBehaviorUV_Bloom
 * @author jinhua.xu
 * @date 2021/5/10 12:04
 * @description 使用布隆过滤器来计算uv
 * @version 1.0
 */
object UserBehaviorUV_Bloom {
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

//    val resultStream: DataStream[UVCount] = dataStream.filter(_.behavior == "pv")
//      .map(data => ("uv", data.userId))
//      .keyBy(_._1)
//      .timeWindow(Time.hours(1))
//      .trigger(new MyTrigger()) //自定义窗口触发规则
//      .process(new UvCountWithBloom())
//
//    resultStream.print()

    env.execute("uv count bloom")
  }
}

// 触发器，每来一条数据直接触发窗口计算并清空窗口状态
//class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
//  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
//
//  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
//
//  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
//}

// 自定义一个布隆过滤器，主要是一个位图和hash函数
class Bloom(size: Long) extends Serializable {
  // 空值布隆过滤器的大小，默认是2的整词幂
  private val cap = size

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0;
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }

    // 返回hash值，要映射到cap范围内

    (cap - 1) & result
  }
}

// 实现自定义的窗口处理函数
//class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UVCount, String, TimeWindow] {
//  // 定义redis连接以及布隆过滤器
//  lazy val jedis = new Jedis("localhost", 6379)
//  lazy val bloomFilter = new Bloom(1 << 29) // 位的个数：64M = 2^6 * 2^20(1M) * 2^3(8bit)
//
//  // 本来是收集器所有数据，窗口触发计算的时候才会调用；现在每来一条数据都调用一次
//  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UVCount]): Unit = {
//
//    // 先定义redis中存储位图的key
//    val storeBitMapKey = context.window.getEnd.toString
//
//    // 将当前窗口的uv count值，作为状态保存到redis里，用一个叫做uvCount的hash表来保存(windowEnd，count)
//    val uvCountMap = "uvcount"
//    val currentKey = context.window.getEnd.toString
//    var count = 0L
//    // 从redis中取出当前窗口的uv count值
//    if (jedis.hget(uvCountMap, currentKey) != null)
//      count = jedis.hget(uvCountMap, currentKey).toLong
//
//    // 去重：判断当前userId的hash值对应的位图位置，是否为0
//    val userId: String = elements.last._2.toString
//
//    // 计算hash值，并对应着位图中的偏移量
//    val offset: Long = bloomFilter.hash(userId, 61)
//
//    // 用redis的位操作命令，取bitmap中对应位的值
//    val isExist: lang.Boolean = jedis.getbit(storeBitMapKey, offset)
//
//    if (!isExist) {
//      // 如果不存在，那么位图对应位置置1，并将count值加1
//      jedis.setbit(storeBitMapKey, offset, true)
//      jedis.hset(uvCountMap, currentKey, (count + 1).toString)
//      out.collect(UVCount(storeBitMapKey.toLong, count + 1))
//    } else {
//      out.collect(UVCount(storeBitMapKey.toLong, count))
//    }
//  }
//}