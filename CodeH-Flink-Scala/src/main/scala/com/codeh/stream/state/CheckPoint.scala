package com.codeh.stream.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @className CheckPoint
 * @author jinhua.xu
 * @date 2021/5/6 10:51
 * @description 检查点功能
 * @version 1.0
 */
object CheckPoint {
  def main(args: Array[String]): Unit = {

    // 1.获取环境变量
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 开启检查点，并一秒钟启动一次
    env.enableCheckpointing(1000L)

    // 设置检查点的模式，最少一次，表示计数结果可能大于正确值
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

    // 设置检查点的超时时间
    env.getCheckpointConfig.setCheckpointTimeout(60000L)

    // 同一时间允许进行两个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

    // 设置检查点之间有至少500ms的间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    //env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)


    /**
     * 重启策略：
     *    参数1：重启次数
     *    参数2：重启的间隔时间
     */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000))

    /**
     * 参数1： 重启次数
     * 参数2： 故障时间间隔
     * 参数3： 延迟重启的时间间隔
     */
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.of(5, TimeUnit.MINUTES), Time.of(3, TimeUnit.MINUTES)))
    env.execute("checkpoint")
  }

}
