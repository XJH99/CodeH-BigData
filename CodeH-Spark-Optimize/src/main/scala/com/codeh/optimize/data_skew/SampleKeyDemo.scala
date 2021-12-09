package com.codeh.optimize.data_skew

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @className SampleKeyDemo
 * @author jinhua.xu
 * @date 2021/12/9 16:27
 * @description 数据倾斜定位大数据量key的方式
 * @version 1.0
 */
object SampleKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BigJoinDemo")
      .set("spark.sql.shuffle.partitions", "36")
      .setMaster("local[*]")
    val sparkSession: SparkSession = InitUtils.getSparkSession(sparkConf)

    println("=============================================csc courseid sample=============================================")
    val cscTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_shopping_cart","courseid")
    println(cscTopKey.mkString("\n"))

    println("=============================================sc courseid sample=============================================")
    val scTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.sale_course","courseid")
    println(scTopKey.mkString("\n"))

    println("=============================================cp orderid sample=============================================")
    val cpTopKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_pay","orderid")
    println(cpTopKey.mkString("\n"))

    println("=============================================csc orderid sample=============================================")
    val cscTopOrderKey: Array[(Int, Row)] = sampleTopKey(sparkSession,"sparktuning.course_shopping_cart","orderid")
    println(cscTopOrderKey.mkString("\n"))

  }

  /**
   * 抽样定位大数据量key
   * @param spark
   * @param table_name
   * @param keyCol
   * @return
   */
  def sampleTopKey(spark: SparkSession, table_name: String, keyCol: String): Array[(Int, Row)] = {
    val df: DataFrame = spark.sql("select " + keyCol + " from " + table_name)
    val tuples: Array[(Int, Row)] = df.select(keyCol)
      .sample(false, 0.1).rdd // 对key进行不放回的采样
      .map(row => (row, 1)).reduceByKey(_ + _) // 统计不同key出现的次数
      .map(row => (row._2, row._1)).sortByKey(false) // 统计的key倒序排序
      .take(10)

    tuples
  }

}
