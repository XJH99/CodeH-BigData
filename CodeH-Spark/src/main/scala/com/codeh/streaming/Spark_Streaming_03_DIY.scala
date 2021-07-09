package com.codeh.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

/**
 * @className Spark_Streaming_03_DIY
 * @author jinhua.xu
 * @date 2021/4/25 10:28
 * @description 自定义数据源
 * @version 1.0
 */
object Spark_Streaming_03_DIY {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Streaming_03_DIY").setMaster("local[*]")

    val sc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 接收自定义数据源的数据
    val res: ReceiverInputDStream[String] = sc.receiverStream(new MyReceiver)

    res.print()

    sc.start()
    sc.awaitTermination()

  }
}

class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // 全局标识变量
  private var flag = true

  /**
   * 最初启动时，调用该方法
   */
  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (flag) {
          val message = "采集的数据为：" + new Random().nextInt(10).toString
          // 单个数据存入到内存中，之后汇总推送到spark
          store(message)
          Thread.sleep(500)
        }
      }
    }).start()
  }

  /**
   * 停止采集数据的方法
   */
  override def onStop(): Unit = {
    flag = false
  }
}
