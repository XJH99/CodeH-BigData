package com.codeh.optimize.cache

import com.codeh.optimize.bean.CoursePay
import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * @className RddCacheKryo
 * @author jinhua.xu
 * @date 2021/11/30 16:15
 * @description rdd缓存 + kryo序列化（MEMORY_ONLY_SER）  ==> 相比于memory缓存节省了大约七倍的内存空间
 * @version 1.0
 */
object RddCacheKryo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RddCacheKryo")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[CoursePay]))
      .setMaster("local[*]")

    val spark: SparkSession = InitUtils.getSparkSession(conf)

    import spark.implicits._
    val rdd: RDD[CoursePay] = spark.sql("select * from sparktuning.course_pay").as[CoursePay].rdd

    rdd.persist(StorageLevel.MEMORY_ONLY_SER)

    rdd.foreachPartition(row => row.foreach(item => println(item.orderid)))

    spark.stop()

  }

}
