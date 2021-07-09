package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark11_SortBy
 * @author jinhua.xu
 * @date 2021/4/19 14:58
 * @description 排序
 * @version 1.0
 */
object Spark11_SortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark11_SortBy")

    val sc = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 8, 4, 6, 3, 5))

    // TODO sortBy对元素进行排序,第二个参数可以改变排序的方式,还有第三个参数改变分区
    // true是升序，false是降序
    val res: RDD[Int] = dataRDD.sortBy(data => data, true)

    println(res.collect().mkString(","))

    sc.stop()
  }

}
