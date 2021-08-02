package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * @className SparkSQL04
 * @author jinhua.xu
 * @date 2021/8/2 14:56
 * @description 自定义DataFrame结构
 * @version 1.0
 */
object SparkSQL04 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL_04").setMaster("local[*]")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark sql basic example")
      .config(conf)
      .getOrCreate()

    val peopleRDD: RDD[String] = spark.sparkContext.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\person.txt")

    val schema = "name age"

    val fields: Array[StructField] = schema.split(" ").map(data => StructField(data, StringType, nullable = true))

    val structType: StructType = StructType(fields)

    val rowRDD: RDD[Row] = peopleRDD.map(_.split(",")).map(data => Row(data(0), data(1).trim))

    val df: DataFrame = spark.createDataFrame(rowRDD, structType)

    df.createOrReplaceTempView("people")

    spark.sql("select name from people").show()

    spark.stop()
  }
}
