package com.codeh.optimize.cache

import com.codeh.optimize.bean.CoursePay
import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @className DatasetCacheSer
 * @author jinhua.xu
 * @date 2021/11/30 16:40
 * @description Dataset缓存 + ser序列化 ==> 内存 + 磁盘 + 序列化
 * @version 1.0
 */
object DatasetCacheSer {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("DatasetCacheSer")
      .setMaster("local[*]")


    val spark: SparkSession = InitUtils.getSparkSession(sparkConf)
    import spark.implicits._

    val ds: Dataset[CoursePay] = spark.sql("select * from sparktuning.course_pay").as[CoursePay]

    ds.persist(StorageLevel.MEMORY_AND_DISK_SER)

//    ds.foreachPartition(row => row.foreach(item => println(item.orderid)))

    spark.stop()
  }
}
