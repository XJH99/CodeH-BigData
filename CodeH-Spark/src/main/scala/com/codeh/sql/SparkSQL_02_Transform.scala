package com.codeh.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @className SparkSQL_02_Transform
 * @author jinhua.xu
 * @date 2021/4/23 14:35
 * @description RDD,DataFrame,DataSet转换
 * @version 1.0
 */
object SparkSQL_02_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_02_Transform")

    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 2.加入隐式转换
    import spark.implicits._

    // 3.创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40)))

    // 4.转换为DF
    val df: DataFrame = rdd.toDF("id", "name", "age")

    // 5.转换为DS
    val ds: Dataset[user] = df.as[user]

    // 6.转换为rdd
    val ds_rdd: RDD[user] = ds.rdd
    val df_rdd: RDD[Row] = df.rdd

    spark.stop()
  }

}

case class user(id: Int, name: String, age: Int)
