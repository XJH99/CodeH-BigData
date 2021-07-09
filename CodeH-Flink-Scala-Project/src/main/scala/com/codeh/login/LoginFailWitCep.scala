package com.codeh.login

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @className LoginFailWitCep
 * @author jinhua.xu
 * @date 2021/5/11 14:20
 * @description 使用CEP来检测恶意登陆的情况
 * @version 1.0
 */
object LoginFailWitCep {
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

    // 1.定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 2.在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(mapStream.keyBy(_.userId), loginFailPattern)

    // 3.select方法传入一个pattern select function，当检测到定义好的模式序列时就会调用
    val result: DataStream[LoginFailWarning] = patternStream.select(new LoginFailEventMatch())

    result.print()

    env.execute("LoginFailWitCep")
  }
}

class LoginFailEventMatch extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    // 当前匹配到的事件序列，就保存到Map中
    val firstFailEvent = map.get("begin").get(0)
    val secondFailEvent = map.get("next").iterator().next()
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, secondFailEvent.timestamp, "login fail 2s")
  }
}
