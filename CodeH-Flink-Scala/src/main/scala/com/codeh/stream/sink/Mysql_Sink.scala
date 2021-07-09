package com.codeh.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.codeh.stream.source.{MySensorSource, SensorReading}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @className Mysql_Sink
 * @author jinhua.xu
 * @date 2021/4/28 17:09
 * @description 将mysql作为输出源
 * @version 1.0
 */
object Mysql_Sink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dataStream: DataStream[SensorReading] = env.addSource(new MySensorSource())

    dataStream.addSink(new MyJdbcSink())

    env.execute("mysql-sink")
  }

}

// 自定义JDBC sink
class MyJdbcSink extends RichSinkFunction[SensorReading] {

  // 定义一些初始化变量
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化操作,创建mysql连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://192.168.214.136:3306/test?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "codeH0000/")
    insertStmt = conn.prepareStatement("INSERT INTO Sensor(id, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement("UPDATE Sensor SET temp = ? WHERE id = ?")
  }

  // 调用连接，执行业务操作
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setString(2, value.id)
    updateStmt.setDouble(1, value.temperature)
    updateStmt.execute()

    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setDouble(2, value.temperature)
      insertStmt.setString(1, value.id)
      insertStmt.execute()
    }
  }

  // 关闭操作
  override def close(): Unit = {

    updateStmt.close()
    insertStmt.close()
    conn.close()
  }
}
