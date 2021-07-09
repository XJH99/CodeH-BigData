package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @className Spark_Streaming_02_Queue
 * @author jinhua.xu
 * @date 2021/4/23 18:57
 * @description
 * @version 1.0
 */
object Spark_Streaming_02_Queue {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_02_Queue").setMaster("local[*]")


    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 1.创建一个队列
    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    // 3.监听这个队列
    val inputStream: InputDStream[Int] = sc.queueStream(queue, oneAtATime = false)

    // 4.wordCount处理
    val res: DStream[(Int, Int)] = inputStream.map((_, 1)).reduceByKey(_ + _)

    res.print()

    sc.start()

    // 2.循环创建rdd，加入到队列中
    for (i <- 1 to 5) {
      queue += sc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    sc.awaitTermination()
  }

}
