package com.codeh.streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @className Spark_Streaming_09_Close
 * @author jinhua.xu
 * @date 2021/4/25 16:18
 * @description 流式任务优雅的关闭
 * @version 1.0
 */
object Spark_Streaming_09_Close {
  def main(args: Array[String]): Unit = {
    val sc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => createSC())

    new Thread(new MonitorStop(sc)).start()
    sc.start()
    sc.awaitTermination()
  }

  def createSC(): _root_.org.apache.spark.streaming.StreamingContext = {
    val update: (Seq[Int], Option[Int]) => Some[Int] = (values: Seq[Int], status: Option[Int]) => {
      //当前批次内容的计算
      val sum: Int = values.sum
      //取出状态信息中上一次状态
      val lastStatu: Int = status.getOrElse(0)
      Some(sum + lastStatu)
    }
    val sparkConf: SparkConf = new
        SparkConf().setMaster("local[*]").setAppName("SparkTest")
    //设置优雅的关闭
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("cp")
    val line = ssc.socketTextStream("192.168.214.136", 9999)
    val word: DStream[String] = line.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = word.map((_, 1))
    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)
    wordAndCount.print()
    ssc
  }

}


class MonitorStop(sc: StreamingContext) extends Runnable {
  override def run(): Unit = {
    // 获取hdfs文件系统实例对象
    val fs: FileSystem = FileSystem.get(new URI("hdfs://192.168.214.136:9000"), new Configuration(), "root")

    while (true) {
      try {
        Thread.sleep(5000)
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }

      // 返回当前上下文的状态
      val state: StreamingContextState = sc.getState()

      val bool: Boolean = fs.exists(new Path("hdfs://192.168.214.136:9000/stopSpark"))

      if (bool) {
        // 状态为活跃状态，关闭
        if (state == StreamingContextState.ACTIVE) {
          sc.stop(stopSparkContext = true, stopGracefully = true)
          System.exit(0)
        }
      }
    }
  }
}
