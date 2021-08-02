package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @className SparkSQL03
 * @author jinhua.xu
 * @date 2021/8/2 14:31
 * @description 与RDD的交互
 * @version 1.0
 */
object SparkSQL03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL_03").setMaster("local[*]")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark sql basic example")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val rdd: RDD[String] = spark
      .sparkContext
      .textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\person.txt")

    val df: DataFrame = rdd.map(
      data => {
        val arr: Array[String] = data.split(",")
        Person(arr(0), arr(1).toInt)
      }
    ).toDF()

    df.createOrReplaceTempView("people")

    val teenagersDF: DataFrame = spark.sql("select name, age from people where age between 10 and 40")

    // 通过下标索引取值
    teenagersDF.map(data => "name: " + data(0)).show()

    // 通过列名进行取值
    teenagersDF.map(data => "name: " + data.getAs[String]("name")).show()

    teenagersDF.map(data => data.getValuesMap(List("name", "age"))).collect()

    spark.stop()
  }
}
