package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @className Spark_Streaming_01_WordCount
 * @author jinhua.xu
 * @date 2021/4/23 18:16
 * @description demo测试
 * @version 1.0
 */
object Spark_Streaming_01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_01_WordCount").setMaster("local[*]")

    /**
     * 1.获取StreamingContext上下文对象
     *    Seconds(3)：数据流批处理的时间间隔
     */
    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 2.监控该ip对应端口的数据，一行一行读取
    val lineStream: ReceiverInputDStream[String] = sc.socketTextStream("192.168.214.136", 9099)

    // 3.wordCount操作
    val res: DStream[(String, Int)] = lineStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 4.打印控制台
    res.print()

    // 5.启动数据采集器
    sc.start()
    sc.awaitTermination() // 等待执行停止
  }

}
