package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @className Spark_Streaming_08_Window
 * @author jinhua.xu
 * @date 2021/4/25 15:48
 * @description 窗口功能
 * @version 1.0
 */
object Spark_Streaming_08_Window {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_08_Window").setMaster("local[*]")

    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 设置检查点操作
    sc.checkpoint("cp")

    val datas = sc.socketTextStream("192.168.214.136", 9999)

    val mapDStream: DStream[(String, Int)] = datas.flatMap(_.split(" ")).map((_, 1))

    /**
     * 第一个参数：正向reduce任务,加上新进入窗口的批次中的元素
     * 第二个参数：逆向reduce任务,移除离开窗口的老批次中的元素
     * 第三个参数：窗口的时长
     * 第四个参数：窗口的滑动步长
     */
    //    val result: DStream[(String, Int)] = mapDStream.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))
    val result: DStream[(String, Int)] = mapDStream.reduceByKeyAndWindow((_ + _), (_ - _), Seconds(12), Seconds(6))
    result.print()

    sc.start()
    sc.awaitTermination()
  }
}
