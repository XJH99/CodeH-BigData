package com.codeh.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

/**
 * @className SparkSQL_Test01
 * @author jinhua.xu
 * @date 2021/9/22 11:13
 * @description 标签结果写出格式
 * @version 1.0
 */
object SparkSQL_Test01 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Test01")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark sql basic example")
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[TagOut] = spark.sql(
      """
        |select
        |     "MEM210325112219838" as one_id,
        |      map("T001", "20211009", "T002", "20211010", "T003", "", "T004", "") as tag
        |""".stripMargin).as[TagOut]
    //map( "T003", "") as tag
    //ds.show(false)

    val value: Dataset[(String, String)] = ds.map(row => {
      val builder: StringBuilder = new StringBuilder()
      builder.append("{")
      for (tag <- row.tag) {
        //val str: String = String.join("\"", "", tag._1, ":[", tag._2, "]")
        var str: String = null
        if (tag._2 == "" || tag._2.isEmpty) {
          str = String.join("\"", "", tag._1, ":[]")
        } else {
          str = String.join("\"", "", tag._1, ":[", tag._2, "]")
        }
        builder.append(str)
        builder.append(",")
      }

      var tagStr: String = builder.mkString
      tagStr = tagStr.substring(0, tagStr.length - 1) + "}"
      (row.one_id, tagStr)
    })


    value.show(false)

    spark.stop()

  }

  case class TagOut(one_id: String, tag: mutable.HashMap[String, String])

}
