package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @className SparkSQL_DataSource
 * @author jinhua.xu
 * @date 2021/12/24 15:13
 * @description spark读取各种数据源操作
 * @version 1.0
 */
object SparkSQL_DataSource {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL_DataSource").setMaster("local[*]")
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    // spark读取数据时，可以忽略损坏的文件
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    // spark读取数据时，忽略丢失的文件
    spark.sql("set spark.sql.files.ignoreMissingFilesDataFrame=true")

    val frame = readCSV(spark)


    frame.show(false)

    // 写出数据文件
    //frame.select("age", "name").write.format("parquet").save("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\people.parquet")

    spark.stop()
  }

  /**
   * 读取csv文件
   * @param spark
   * @return
   */
  def readCSV(spark: SparkSession): DataFrame = {
    val frame: DataFrame = spark.read.format("csv")
      .option("sep", ",") // 数据分隔符
      .option("inferSchema", "true")
      .option("header", "false")  // 是否包含表头
      .load("CodeH-Spark/input/score.csv")

    frame
  }

  def readJson(spark: SparkSession): DataFrame = {
    val frame: DataFrame = spark.read.format("json").load("CodeH-Spark/input/user.json")
    frame
  }
}
