package com.codeh.optimize.cache

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @className RddCache
 * @author jinhua.xu
 * @date 2021/11/29 17:06
 * @description RDD缓存处理：默认使用的是MEMORY_ONLY级别的缓存
 * @version 1.0
 */
object RddCache {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("RddCache")
      .setMaster("local[*]")

    val spark: SparkSession = InitUtils.getSparkSession(sparkConf)

    val rdd: RDD[Row] = spark.sql("select * from sparktuning.course_pay").rdd

    // 缓存处理
    rdd.cache()

    rdd.foreachPartition(row => row.foreach(item => println(item.get(0))))

  }
}
