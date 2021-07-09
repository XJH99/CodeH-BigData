package com.codeh.rdd.persistence

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @className Spark_Persistence
 * @author jinhua.xu
 * @date 2021/4/22 17:51
 * @description Spark的持久化操作
 * @version 1.0
 */
object Spark_Persistence {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Persistence")

    val sc = new SparkContext(sparkConf)

    // 设置检查点路径
    sc.setCheckpointDir("checkpoint/")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    mapRDD.persist(StorageLevel.MEMORY_ONLY) // 将rdd持久化到内存中

    mapRDD.checkpoint() // 对mapRDD做检查点计算

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_ + _)

    println(reduceRDD.toDebugString)

    // 对RDD进行checkpoint操作并不会马上执行，必须执行行动算子才会触发
    println(reduceRDD.collect())

    sc.stop()
  }
}
