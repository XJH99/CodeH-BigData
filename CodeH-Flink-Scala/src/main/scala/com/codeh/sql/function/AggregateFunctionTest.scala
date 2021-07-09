package com.codeh.sql.function

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
 * @className AggregateFunctionTest
 * @author jinhua.xu
 * @date 2021/5/7 18:44
 * @description 自定义聚合函数
 * @version 1.0
 */
object AggregateFunctionTest {
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

    // 创建聚合函数实例
    val avgTemp: AvgTemp = new AvgTemp()

    // Table Api进行调用
    val resultTable: Table = sensorTable.groupBy('id)
      .aggregate(avgTemp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)

    // Table sql进行实现
    // 创建实例，并注册
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTemp)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select
        | id, avgTemp(temperature)
        |from sensor
        |group by id
        |""".stripMargin)

    // 打印输出
    resultTable.toRetractStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("sql")

    env.execute("AggregateFunctionTest")
  }

}

// 自定义聚合函数，计算每个sensor的平均温度
class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  // 获取计算后的结果:平均值
  override def getValue(accumulator: AvgTempAcc): Double = {
    accumulator.sum / accumulator.count
  }

  // 创建空的累加器
  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  // 更新累加器数值
  def accumulate(accumulator: AvgTempAcc, temp: Double): Unit = {
    accumulator.sum += temp
    accumulator.count += 1
  }
}

// 定义Accumulator
class AvgTempAcc {
  var sum: Double = _ // 总和
  var count: Int = _ // 总数
}
