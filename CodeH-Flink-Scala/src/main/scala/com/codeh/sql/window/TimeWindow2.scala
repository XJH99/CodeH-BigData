package com.codeh.sql.window

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @className TimeWindow2
 * @author jinhua.xu
 * @date 2021/5/7 18:06
 * @description window的api测试
 * @version 1.0
 */
object TimeWindow2 {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2.读取数据
    val path: String = "D:\\IDEA_Project\\flink_scala\\src\\main\\resources\\sensor.txt"
    val ds: DataStream[String] = env.readTextFile(path)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    })

    // 将流转为table
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // Group Window
    // 4.1分组窗口
    val resTable: Table = sensorTable.window(Tumble over 10.seconds on 'ts as 'tw) // 每10秒统计一次，滚动事件窗口
      .groupBy('id, 'tw) // 分组必须加上上面定义的窗口
      .select('id, 'id.count, 'temperature.avg, 'tw.end)

    // 4.2 sql
    tableEnv.createTemporaryView("sensor", sensorTable)

    val resSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        | id, count(id), avg(temperature), tumble_end(ts, interval '10' second)
        |from sensor
        |group by id, tumble(ts, interval '10' second)
        |
        |""".stripMargin)

    // 5. Over Window：统计每个Sensor每条数据，与之前两行数据的平均温度
    // 5.1 table api
    val overResultTable: Table = sensorTable.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)

    // 5.2 sql
    val overResultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        | id, ts, count(id) over ow, avg(temperature) over ow
        |from sensor
        |window ow as (
        |   partition by id
        |   order by ts
        |   rows between 2 preceding and current row
        |)
        |""".stripMargin)

    // 打印输出
    //    resTable.toAppendStream[Row].print("resTable")
    //    resSqlTable.toRetractStream[Row].print("sql")
    overResultTable.toAppendStream[Row].print("overResultTable")
    overResultSqlTable.toRetractStream[Row].print("overResultSqlTable")


    env.execute("window test")
  }

}
