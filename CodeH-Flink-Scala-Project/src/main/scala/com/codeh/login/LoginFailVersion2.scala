package com.codeh.login

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @className LoginFailVersion2
 * @author jinhua.xu
 * @date 2021/5/11 11:56
 * @description 恶意登陆监控版本2
 * @version 1.0
 */
object LoginFailVersion2 {
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
      .process(new LoginFailProcessVersion2())

    result.print()

    env.execute("LoginFailDetect-Version2")
  }

}

class LoginFailProcessVersion2() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {

  // 定义状态变量, 失败的状态
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailState", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {

    // 当为失败状态时
    if (value.eventType == "fail") {
      val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
      if (iter.hasNext) {
        val firstFail = iter.next()
        // 如果两次登陆时间小于2s，输出报警
        if (value.timestamp < firstFail.timestamp + 2) {
          out.collect(LoginFailWarning(value.userId, firstFail.timestamp, value.timestamp, "login fail in 2 seconds"))
        }
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        loginFailState.add(value)
      }
    } else {
      loginFailState.clear()
    }

  }
}
