package com.codeh.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark_BroadCast
 * @author jinhua.xu
 * @date 2021/4/23 11:27
 * @description 广播变量：分布式共享只读变量
 * @version 1.0
 */
object Spark_BroadCast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_BroadCast")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 4)

    val list = List(("a",4), ("b", 5), ("c", 6), ("d", 7))

    // 1.声明广播变量
    val bc: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    // 2.executor端数据处理,进行数据关联
    val result: RDD[(String, (Int, Int))] = rdd.map {
      case (key, num) => {
        var tmp = 0
        for ((k, v) <- bc.value) {
          if (key == k) {
            tmp = v
          }
        }
        (key, (num, tmp))
      }
    }

    // 3.遍历打印操作
    for ((key,(v1,v2)) <- result.collect()) {
      println(key + " == " + (v1,v2))
    }

    sc.stop()
  }
}
