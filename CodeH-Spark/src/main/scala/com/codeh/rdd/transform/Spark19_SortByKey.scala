package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark19_SortByKey
 * @author jinhua.xu
 * @date 2021/4/19 15:49
 * @description 通过key进行排序
 * @version 1.0
 */
object Spark19_SortByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark19_SortByKey")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("c", 3), ("b", 2)
    ), 2)

    // 通过 key 来进行排序
    val sortRDD: RDD[(String, Int)] = rdd.sortByKey(true) // true 表示升序

    println(sortRDD.collect().mkString(","))

    sc.stop()
  }
}
