package com.codeh.stream.source

import org.apache.flink.streaming.api.scala._

/**
 * @className PortSource
 * @author jinhua.xu
 * @date 2021/4/28 16:04
 * @description 从端口读取数据
 * @version 1.0
 */
object PortSource {
  def main(args: Array[String]): Unit = {
    // 1.获取流式环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    // 2.端口读取数据
    val ds: DataStream[String] = env.socketTextStream("192.168.214.136", 9999)

    // 3.打印数据
    ds.print()

    // 4.执行程序
    env.execute("Port Source")

  }

}
