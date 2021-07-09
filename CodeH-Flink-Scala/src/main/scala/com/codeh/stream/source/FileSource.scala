package com.codeh.stream.source

import org.apache.flink.streaming.api.scala._

/**
 * @className FileSource
 * @author jinhua.xu
 * @date 2021/4/28 15:56
 * @description 从文件中读取数据
 * @version 1.0
 */
object FileSource {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 2.从文件中读取数据
    val path: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Scala\\src\\main\\resources\\sensor.txt"
    val ds: DataStream[String] = env.readTextFile(path)

    val result: DataStream[SensorReading] = ds.map(
      data => {
        val datas = data.split(",")
        SensorReading(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    // 3.打印数据
    result.print()

    // 4.执行程序
    env.execute("File Source")
  }

}
