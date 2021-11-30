package com.codeh.optimize.cache

import com.codeh.optimize.bean.CoursePay
import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @className DatasetCache
 * @author jinhua.xu
 * @date 2021/11/30 16:36
 * @description Dataset缓存：使用的缓存为MEMORY_AND_DISK
 * @version 1.0
 */
object DatasetCache {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("DatasetCache")
      .setMaster("local[*]")

    val spark: SparkSession = InitUtils.getSparkSession(sparkConf)

    import spark.implicits._
    val ds: Dataset[CoursePay] = spark.sql("select * from sparktuning.course_pay").as[CoursePay]

    ds.cache()

    ds.foreachPartition(row => row.foreach(item => println(item.orderid)))

    spark.stop()

  }

}
