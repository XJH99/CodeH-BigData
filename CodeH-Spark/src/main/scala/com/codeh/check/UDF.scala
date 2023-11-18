package com.codeh.check

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct, to_json, udf}

import scala.collection.mutable.ListBuffer

/**
 * @author jinhua.xu
 * @date 2023/5/27 20:11
 * @version 1.0
 * @describe
 */
object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("UDF")
      .master("local[*]")
      .getOrCreate()

    spark.sql(
      """
        |SELECT CASE WHEN (dfc_postcode IS NOT NULL) THEN (CASE
        |                                                    WHEN dfc_postcode LIKE 'CN%' AND substring(dfc_postcode, 3) RLIKE '^[0-9]{2}$' THEN 'yes'
        |                                                    ELSE 'not valid' END )
        |            ELSE 'yes' END AS col
        |from
        |    (SELECT 'CD34' AS dfc_postcode) AS t1
        |""".stripMargin).show(false)


  }

}
