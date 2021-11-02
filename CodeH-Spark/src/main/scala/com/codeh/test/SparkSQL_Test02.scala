package com.codeh.test

import java.util.concurrent.{Callable, Executor, ExecutorService, Executors, Future, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @className SparkSQL_Test02
 * @author jinhua.xu
 * @date 2021/11/1 17:43
 * @description 测试callable并发情况下的线程重试机制
 * @version 1.0
 */
object SparkSQL_Test02 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Test02")
    val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val service: ExecutorService = Executors.newFixedThreadPool(10)
    val listBuffer: ListBuffer[Future[Int]] = mutable.ListBuffer()

    for (i <- 1 to 10) {
      var failure_num = 0
      getCallBack(i, service, failure_num, listBuffer)
    }

    var flag1 = true
    while (flag1) {
      val list = listBuffer.toList
      var count = 0;
      for (future <- list) {
        if (future.isDone) {
          listBuffer.remove(count)
          count -= 1
        }

        count += 1
        // all tags has been calculated
        if (listBuffer.isEmpty) {
          flag1 = false
          service.shutdown()
        }
      }
    }

    spark.stop()
  }

  private def getCallBack(i: Int, service: ExecutorService, failure_num: Int, listBuffer: ListBuffer[Future[Int]]): Unit = {
    var fail_count = failure_num
    val future: Future[Int] = service.submit(new Callable[Int] {
      override def call(): Int = {
        println("当前处理的标签为" + i + ",当前处理的线程名称为：" + Thread.currentThread().getName)
        if (i == 8) {
          i % 0
        } else {
          i
        }
      }
    })

    try {
      future.get()
      listBuffer += future
    } catch {
      case e: Exception=> {
        if (fail_count < 2) {
          fail_count += 1
          println("标签" + i + "计算失败，进行第" + fail_count + "次重试")
          getCallBack(i, service, fail_count, listBuffer)
        } else {
          service.shutdown()
          throw new Exception("标签" + i + "计算出错，整个job失败")
        }
      }
    }
  }
}
