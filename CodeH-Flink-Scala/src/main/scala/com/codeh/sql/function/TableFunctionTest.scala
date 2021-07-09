package com.codeh.sql.function

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * @className TableFunctionTest
 * @author jinhua.xu
 * @date 2021/5/7 18:35
 * @description 表函数:可以将 0、1 或多个标量值作为输入参数，它可以返回任意数量的行作为输出，而不是单个值
 * @version 1.0
 */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    // 创建流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表的环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2.读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt"
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

    // table api 使用
    // 常见split实例
    val split = new Split("_")

    val resultTable: Table = sensorTable.joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)

    // sql的使用
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        |  id, word, length
        |from sensor, lateral table(split(id)) as newSensor(word, length)
        |""".stripMargin)

    // 数据写出
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("sql")

    env.execute(" table Function ")
  }

}

// 自定义表函数
class Split(sep: String) extends TableFunction[(String, Int)] {

  // 功能实现
  def eval(str: String): Unit = {
    str.split(sep).foreach(
      word => collect((word, word.length))
    )
  }
}
