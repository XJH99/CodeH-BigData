package com.codeh.sql.function

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * @className TableAggregateFunctionTest
 * @author jinhua.xu
 * @date 2021/5/7 18:51
 * @description 自定义表聚合函数：提取每个sensor最高的两个温度值
 * @version 1.0
 */
object TableAggregateFunctionTest {
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

    // 创建表聚合函数实例
    val top2Temp = new Top2Temp()

    // Table API调用
    val resultTable: Table = sensorTable.groupBy('id)
      .flatAggregate(top2Temp('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)

    // 转化为流打印
    resultTable.toRetractStream[Row].print("result")

    env.execute("TableAggregateFunctionTest")
  }

}

// 定义一个Accumulator
class Top2TempAcc {
  var firstTemp: Double = Int.MinValue
  var secondTemp: Double = Int.MinValue
}

class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  // 创建一个累加器
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  // 温度判断，逻辑处理
  def accumulate(accTop2: Top2TempAcc, temp: Double): Unit = {
    if (temp > accTop2.firstTemp) {
      accTop2.secondTemp = accTop2.firstTemp
      accTop2.firstTemp = temp
    } else if (temp > accTop2.secondTemp) {
      accTop2.secondTemp = temp
    }
  }

  // 返回结果
  def emitValue(accTop2: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
    out.collect(accTop2.firstTemp, 1)
    out.collect(accTop2.secondTemp, 2)
  }
}

