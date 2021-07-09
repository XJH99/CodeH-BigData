package com.codeh.networkflow

import com.codeh.hotitem.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @className UserBehaviorPV
 * @author jinhua.xu
 * @date 2021/5/10 11:11
 * @description 网站的pv数量统计
 * @version 1.0
 */
object UserBehaviorPV {
  def main(args: Array[String]): Unit = {
    // 获取环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\UserBehavior.csv"
    val fileStream: DataStream[String] = env.readTextFile(path)

    // 全网的pv计算
    val resultStream: DataStream[(String, Int)] = fileStream.map(
      data => {
        val lines: Array[String] = data.split(",")
        UserBehavior(lines(0).toLong, lines(1).toLong, lines(2).toLong, lines(3), lines(4).toLong)
      }
    ).assignAscendingTimestamps(_.ts * 1000L)
      .filter(_.behavior == "pv")
      .map(x => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60 * 60))
      .sum(1)

    resultStream.print()


    env.execute("PV Executor")
  }

}

// 样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, ts: Long)