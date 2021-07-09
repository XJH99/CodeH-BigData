package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @className Spark_Streaming_06_Join
 * @author jinhua.xu
 * @date 2021/4/25 15:28
 * @description 两个数据流做join操作
 * @version 1.0
 */
object Spark_Streaming_06_Join {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_06_Join").setMaster("local[*]")

    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val input1: ReceiverInputDStream[String] = sc.socketTextStream("192.168.214.136", 8801)
    val input2: ReceiverInputDStream[String] = sc.socketTextStream("192.168.214.136", 8802)

    val transformDStream1: DStream[(String, Int)] = input1.flatMap(_.split(" ")).map((_, 1))
    val transformDStream2: DStream[(String, String)] = input2.flatMap(_.split(" ")).map((_, "a"))

    // 两个流做join
    val joinDStream: DStream[(String, (Int, String))] = transformDStream1.join(transformDStream2)

    joinDStream.print()

    sc.start()
    sc.awaitTermination()
  }
}
