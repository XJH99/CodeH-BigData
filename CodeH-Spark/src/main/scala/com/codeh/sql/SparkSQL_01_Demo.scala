package com.codeh.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @className SparkSQL_01_Demo
 * @author jinhua.xu
 * @date 2021/4/23 14:26
 * @description Spark SQL案例
 * @version 1.0
 */
object SparkSQL_01_Demo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01_Demo")

    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 2.读取数据,得到DataFrame对象
    val df: DataFrame = spark.read.json("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\user.json")

    df.show()

    // 3.创建一张零时表
    df.createOrReplaceTempView("user")

    // 4.使用spark sql进行查询
    spark.sql(
      """
        |select *
        |from user
        |where age = '20'
        |""".stripMargin).show(false)

    // 关闭SparkSession对象
    spark.stop()
  }

}
