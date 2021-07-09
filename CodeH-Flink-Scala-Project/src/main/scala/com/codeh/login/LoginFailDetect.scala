package com.codeh.login

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, StateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @className LoginFailDetect
 * @author jinhua.xu
 * @date 2021/5/11 10:29
 * @description 检测登陆短时间内失败多次的用户
 * @version 1.0
 */
object LoginFailDetect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\LoginLog.csv")

    val mapStream: DataStream[LoginEvent] = dataStream.map(
      data => {
        val arr: Array[String] = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
      override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
    })

    val result: DataStream[LoginFailWarning] = mapStream.keyBy(_.userId)
      .process(new LoginFailProcess(2))

    result.print()

    env.execute("LoginFailDetect")
  }
}


class LoginFailProcess(i: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  // 定义状态变量, 失败的状态，定时器时间的状态
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailState", classOf[LoginEvent]))

  lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))

  // 业务处理
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
      if (value.eventType == "fail") {
        loginFailState.add(value)

        // 如果不存在定时器，创建一个
        if (timerState.value() == 0) {
          val ts = value.timestamp * 1000L + 2000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else {
        // 成功的情况下，清空状态，清空定时器
        ctx.timerService().deleteEventTimeTimer(timerState.value())
        loginFailState.clear()
        timerState.clear()
      }
  }

  // 触发定时器操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val failList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()

    // 获取失败列表迭代器
    val iter = loginFailState.get().iterator()
    while (iter.hasNext) {
      val event: LoginEvent = iter.next()
      failList += event
    }

    // 大于失败的上限
    if (failList.length > i) {
      out.collect(LoginFailWarning(failList.head.userId, failList.head.timestamp, failList.last.timestamp, "login fail in 2s for " + failList.length + " times"))
    }

    // 清空状态
    loginFailState.clear()
    timerState.clear()
  }
}

// 输入的登陆事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)

// 输出报警的样例类
case class LoginFailWarning(userId: Long, firstFailTime: Long, lastFailTime:Long, warningMsg: String)