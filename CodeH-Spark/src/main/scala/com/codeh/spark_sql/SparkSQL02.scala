package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @className SparkSQL02
 * @author jinhua.xu
 * @date 2021/8/2 14:17
 * @description spark sql dataset
 * @version 1.0
 */
object SparkSQL02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkSQL_02").setMaster("local[*]")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark sql basic example")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    //    val ds: Dataset[Person] = Seq(Person("Andy", 20)).toDS()

    //    val ds: Dataset[Int] = Seq(1, 2, 3).toDS()
    //
    //    val arr: Array[Int] = ds.map(_ + 1).collect()

    val ds: Dataset[Person] = spark.read.json("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\user.json").as[Person]

    ds.show()

    spark.stop()

  }
}

case class Person(name: String, age: Long)
