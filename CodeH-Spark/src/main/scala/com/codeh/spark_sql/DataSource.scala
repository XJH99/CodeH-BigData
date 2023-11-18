package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author jinhua.xu
 * @date 2023/2/21 18:39
 * @version 1.0
 * @describe
 */
object DataSource {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Spark PostgreSQL Demo")
      .master("local[*]")
      .getOrCreate()

    val url_list = List("jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=three_ce",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=ac",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=bio",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=ca",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=cerave",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=gac",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=hr",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=kie",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=ks",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=public",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=lp",
      "jdbc:postgresql://10.162.65.21:5432/oreal_cpd?currentSchema=cpd_lrl",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=lrp",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=mbb",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=mg",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=mm",
      "jdbc:postgresql://10.162.65.21:5432/oreal_cpd?currentSchema=cpd_mny",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=sgmb",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=shu",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=skc",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=tak",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=ud",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=vch",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=vltn",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=ys",
      "jdbc:postgresql://10.162.65.21:5432/oreal_loyalty?currentSchema=ysl")

    val jdbcUsername = "hypers_swn"
    val jdbcPassword = "hypers_swn_x3_l_2@1"
    val jdbcTable = "wechat_fans"

    for (url <- url_list) {
      val df = spark.read
        .format("jdbc")
        .option("url", url)
        .option("user", jdbcUsername)
        .option("password", jdbcPassword)
        .option("dbtable", jdbcTable)
        .load()

      df.createOrReplaceTempView("wechat_fans")

      spark.sql(
        s"""
           |select "${url}", count(1) from wechat_fans where substr(etl_date, 1, 10) = '2023-04-06'
           |""".stripMargin).show(false)
    }

    spark.stop()
  }
}






