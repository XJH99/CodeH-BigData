package com.codeh.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className SparkSQL_06_Hive
 * @author jinhua.xu
 * @date 2021/4/23 17:03
 * @description Spark关联外部Hive操作
 * @version 1.0
 */
object SparkSQL_06_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkSQL_06_Hive").setMaster("local[*]")

    // 开启hive的支持
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //spark.sql("show databases").show()

    // 这里注意依赖配置的版本问题
    spark.sql("select * from hypers.student").show()

    spark.stop()
  }

}
