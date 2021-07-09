package com.codeh.stream.sink

import com.codeh.stream.source.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * @className File_Sink
 * @author jinhua.xu
 * @date 2021/4/28 16:53
 * @description 输出到文件
 * @version 1.0
 */
object File_Sink {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 2.读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt"
    val ds: DataStream[String] = env.readTextFile(path)

    // 3.将数据进行转化，组成SensorReading样例类格式
    val dataStream: DataStream[SensorReading] = ds.map(data => {
      val datas: Array[String] = data.split(",")
      SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
    })

    // 4.将数据按照csv的格式输出到文件，writeAsCsv默认已弃用
    dataStream.print()
    //dataStream.writeAsCsv("D:\\IDEA_Project\\flink_scala\\src\\main\\resources\\out.txt")
    // 使用addSink来输出数据
    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\out1.txt"),
      new SimpleStringEncoder[SensorReading]()
    ).build())

    env.execute("File_Sink")
  }

}
