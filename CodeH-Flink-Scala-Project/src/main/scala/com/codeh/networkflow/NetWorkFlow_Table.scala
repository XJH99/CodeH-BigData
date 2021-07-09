package com.codeh.networkflow

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @className NetWorkFlow_Table
 * @author jinhua.xu
 * @date 2021/5/10 10:45
 * @description 使用Table API完成基于服务器log的热门页面浏览器统计,统计一段时间内每个url访问用户的人数有多少，之后对结果排序输出
 * @version 1.0
 */
object NetWorkFlow_Table {
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
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala-Project\\src\\main\\resources\\apache.log"

    val dataStream: DataStream[String] = env.readTextFile(path)

    val transformStream: DataStream[ApacheLog] = dataStream.map(
      data => {
        val lines: Array[String] = data.split(" ")
        val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = sdf.parse(lines(3)).getTime // 返回一个毫秒数的时间戳
        ApacheLog(lines(0), lines(2), timestamp, lines(5), lines(6))
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.seconds(1)) { // 乱序程度设置为1s左右
      override def extractTimestamp(element: ApacheLog): Long = element.eventTime // 注意这里的标准是要一个毫秒值
    }) // 乱序数据的处理

    val dataTable: Table = tableEnv.fromDataStream(transformStream, 'ip, 'userId, 'eventTime.rowtime as 'ts, 'method, 'url)

    val aggTable: Table = dataTable.filter('method === "GET")
      .window(Slide over 10.minutes every 5.seconds on 'ts as 'sw) // 10分钟窗口，5秒滑动
      .groupBy('url, 'sw)
      .select('url, 'sw.end as 'windowEnd, 'url.count as 'cnt)


    tableEnv.createTemporaryView("aggTable", aggTable, 'url, 'windowEnd, 'cnt)

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
