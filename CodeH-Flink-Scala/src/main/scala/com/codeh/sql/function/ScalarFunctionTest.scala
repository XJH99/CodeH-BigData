package com.codeh.sql.function

import com.codeh.stream.source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * @className ScalarFunctionTest
 * @author jinhua.xu
 * @date 2021/5/7 18:23
 * @description 自定义标量函数
 * @version 1.0
 */
object ScalarFunctionTest {
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


    // Table API中调用
    val hashCode = new HashCode(20)

    val resultTable: Table = sensorTable.select('id, 'ts, hashCode('id))

    // sql中调用
    // 注册函数
    tableEnv.registerFunction("hashCode", hashCode)
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable: Table = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor")

    // 数据写出
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("sql")

    env.execute(" hashCode Function ")
  }

}

// 自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {

  // 函数名称必须为eval
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }

}
