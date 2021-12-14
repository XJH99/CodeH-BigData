package com.codeh.thread

import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import util.control.Breaks._

/**
 * @className Application
 * @author jinhua.xu
 * @date 2021/12/13 10:45
 * @description 线程池提交任务并发处理，并对线程级别的重试处理
 * @version 1.0
 */
object Application {
  val executorService: ExecutorService = Executors.newFixedThreadPool(10)
  val max_retries: Int = 3
  @volatile var count: Int = 0
  val lock: Object = new Object

  def main(args: Array[String]): Unit = {
    // 创建一个可变map容器，用于存放返回的future
    val map: mutable.Map[TaskMetaData, Future[Int]] = mutable.HashMap[TaskMetaData, Future[Int]]()
    for (i <- 1 to 10) {
      val taskMetaData: TaskMetaData = new TaskMetaData(i, "task-" + i, 0)
      map.put(taskMetaData, submit(2))
    }

    TimeUnit.SECONDS.sleep(2)

    var flag: Boolean = true
    while (flag) {
      val keys: mutable.ListBuffer[TaskMetaData] = ListBuffer[TaskMetaData]()
      for (taskMetaData <- map.keys) {
        keys += taskMetaData
      }

      for (key: TaskMetaData <- keys) {
        val future: Future[Int] = map.get(key).get
        println("task_id: " + key.taskId + " task_name: " + key.taskName + " retries: " + key.retries)
        if (future.isDone) {
          try {
            println(String.format("Done Thread:%s", key.taskName))
            future.get()
            map -= key
          } catch {
            case _: Exception => {
              println(String.format("start resubmit task: %s", key.taskName))
              if (key.retries > max_retries) {
                throw new Exception("test")
              } else {
                key.retries += 1
                println("task_id: " + key.taskId + " task_name: " + key.taskName + " retries: " + key.retries)
                val retryFuture: Future[Int] = submit(2)
                map.put(key, retryFuture)
              }
            }
          }
        }

        if (map.isEmpty) {
          flag = false
        }
      }
    }

    executorService.shutdown()
  }

  def submit(sleepTime: Int): Future[Int] = {
    TimeUnit.SECONDS.sleep(sleepTime)
    val result: Future[Int] = executorService.submit(new Callable[Int] {
      override def call(): Int = {
        for (num <- 1 to 100){
          lock.synchronized {
            breakable {
              count += 1
              println("current thread name " + Thread.currentThread().getName + " count: " + count)
              //              println(String.format("current thread: %s count: %d", Thread.currentThread(), count))
              if (count == 50 || count == 100) {
                println(String.format("[Error] thread: %s", Thread.currentThread().getName))
                //                throw new Exception(String.format("count: %d", count))
                throw new Exception("fail thread name is " + Thread.currentThread().getName)
              }
              if (count > 200) {
                break()
              }
            }
          }
        }
        0
      }
    })

    result
  }
}

class TaskMetaData(task_id: Int, task_name: String, inRetry: Int) {
  var taskId: Int = task_id
  var taskName: String = task_name
  var retries: Int = inRetry
}
