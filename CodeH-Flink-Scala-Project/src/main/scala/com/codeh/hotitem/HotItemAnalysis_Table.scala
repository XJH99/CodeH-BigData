package com.codeh.hotitem

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @className HotItemAnalysis_Table
 * @author jinhua.xu
 * @date 2021/5/8 14:46
 * @description 使用Table API完成每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品
 * @version 1.0
 */
object HotItemAnalysis_Table {
  def main(args: Array[String]): Unit = {
    // 获取流环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 获取表环境对象
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\UserBehavior.csv"

    val dataStream: DataStream[String] = env.readTextFile(path)

    val userBehavior: DataStream[UserBehavior] = dataStream.map(
      data => {
        val datas: Array[String] = data.split(",")
        UserBehavior(datas(0).toLong, datas(1).toLong, datas(2).toInt, datas(3), datas(4).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000)

    // 将dataStream转为Table
    val dataTable: Table = tableEnv.fromDataStream(userBehavior, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 过滤，聚合操作
    val aggTable: Table = dataTable.filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw) // 使用滑动窗口
      .groupBy('itemId, 'sw) // 对产品，窗口进行分组
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt) //得到商品，窗口，总数的数据结构

    // top N的选取
    tableEnv.createTemporaryView("aggTable", aggTable, 'itemId, 'windowEnd, 'cnt)

    tableEnv.sqlQuery(
      """
        |select
        |   *
        |from
        |     (select
        |         *,
        |         row_number() over(partition by windowEnd order by cnt desc) as row_num
        |      from aggTable)
        |where row_num <= 3
        |""".stripMargin).toRetractStream[Row].print()

    env.execute("HotItems Table")
  }

}
