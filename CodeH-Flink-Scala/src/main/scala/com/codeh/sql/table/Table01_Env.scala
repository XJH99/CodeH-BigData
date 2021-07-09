package com.codeh.sql.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

/**
 * @className Table01_Env
 * @author jinhua.xu
 * @date 2021/5/6 11:02
 * @description table环境变量的获取
 * @version 1.0
 */
object Table01_Env {
  def main(args: Array[String]): Unit = {
    // 创建Flink流环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 创建Flink table的环境变量
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    // 1.1 基于老版本planner的流处理
    val oldSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用老版本的planner
      .inStreamingMode()  // 流处理模式
      .build()

    val oldTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, oldSettings)

    // 1.2 基于老版本的批处理
    val oldBatchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(oldBatchEnv)

    // 1.3 基于blink planner的流处理
    val blinkSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() // 执行计划器
      .inStreamingMode()
      .build()

    val blinkTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, blinkSettings)

    // 1.4 基于blink planner的批处理
    val blinkBatchSettings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

    val blinkBatchTableEnv: TableEnvironment = TableEnvironment.create(blinkBatchSettings)
  }

}
