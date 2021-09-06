package com.codeh.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @className SparkSQL_07_Frame
 * @author jinhua.xu
 * @date 2021/9/6 15:34
 * @description TODO
 * @version 1.0
 */
object SparkSQL_07_Frame {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_07_Frame")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark sql basic example")
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\user.json")

    //    val frame: DataFrame = df.withColumn("sex", lit("1"))
    val frame: DataFrame = df.withColumnRenamed("name", "asName")

    frame.show()

    spark.stop()

  }


}
