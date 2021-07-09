package com.codeh.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @className StreamWordCount
 * @author jinhua.xu
 * @date 2021/4/28 15:40
 * @description 流式wordCount统计
 * @version 1.0
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 可以设置并行度，不设置就是当前机器的核心数
    // env.setParallelism(8)

    // 从外部命令中提取参数，作为socket的主机名与端口号
    val parameterTool:ParameterTool = ParameterTool.fromArgs(args)
    val host: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")

    // 2.接收一个socket文本流
    val ds: DataStream[String] = env.socketTextStream(host, port)

    // 3.对数据进行处理
    val res: DataStream[(String, Int)] = ds.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 4.打印输出
    res.print().setParallelism(1) // 可以对当前算子设置并行度

    // 5.启动executor服务,执行任务
    env.execute()
  }

}
