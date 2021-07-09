package com.codeh.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @className Spark09_Coalesce
 * @author jinhua.xu
 * @date 2021/4/18 21:34
 * @description Coalesce:缩减分区功能
 * @version 1.0
 */
object Spark09_Coalesce {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark09_Coalesce")

    val sc = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 1, 1, 6, 6, 6), 6)

    val rdd = dataRDD.filter(item => {
      item % 2 == 0
    })

    // TODO 当数据过滤后，发现数据不均匀，那么可以缩减分区
    //val coalesceRDD = rdd.coalesce(1)

    // TODO 如果发现数据分区不合理，也可以缩减分区
    val coalesceRDD = dataRDD.coalesce(2)

    coalesceRDD.saveAsTextFile("output")

    sc.stop()
  }

}
