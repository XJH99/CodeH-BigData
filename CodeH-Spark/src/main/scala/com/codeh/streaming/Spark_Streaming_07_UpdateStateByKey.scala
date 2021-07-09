package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @className Spark_Streaming_07_UpdateStateByKey
 * @author jinhua.xu
 * @date 2021/4/25 15:37
 * @description 用于维护跨批次状态
 * @version 1.0
 */
object Spark_Streaming_07_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_07_UpdateStateByKey").setMaster("local[*]")

    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val datas = sc.socketTextStream("localhost", 9999)

    val word: DStream[(String, Int)] = datas.map((_, 1))

    /**
     *  updateStateByKey: 根据key对数据进行更新
     *  第一个参数：表示相同key的value数量
     *  第二个参数：表示缓冲区内相同key的value数量
     */
    val state: DStream[(String, Int)] = word.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val res = buff.getOrElse(0) + seq.sum
        Option(res)
      }
    )

    state.print()

    sc.start()
    sc.awaitTermination()

  }

}
