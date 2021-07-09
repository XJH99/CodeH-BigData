package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @className Spark_Streaming_05_Transform
 * @author jinhua.xu
 * @date 2021/4/25 11:24
 * @description Transform允许 DStream 上执行任意的 RDD-to-RDD 函数,每一个批次调度一次
 * @version 1.0
 */
object Spark_Streaming_05_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_05_Transform").setMaster("local[*]")

    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val input: ReceiverInputDStream[String] = sc.socketTextStream("192.168.214.136", 9999)

    /**
     * 对一个批次中的每个rdd进行操作
     */
    val res: DStream[(String, Int)] = input.transform(
      rdd => {
        val words: RDD[String] = rdd.flatMap(_.split(" "))
        val wordToOne: RDD[(String, Int)] = words.map((_, 1))
        wordToOne.reduceByKey(_ + _)
      }
    )

    res.print()

    sc.start()
    sc.awaitTermination()
  }
}
