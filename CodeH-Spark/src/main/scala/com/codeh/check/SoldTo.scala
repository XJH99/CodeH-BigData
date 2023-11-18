package com.codeh.check

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author jinhua.xu
 * @date 2023/5/26 19:03
 * @version 1.0
 * @describe
 */
object SoldTo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SoldTo")
      .master("local[*]")
      .getOrCreate()


    spark.sql(
      """
        |select 'tom' as col1,
        |       null as col2,
        |       '1' as col3,
        |       'noTrue' as col4
        |union all
        |select 'jack' as col1,
        |       null as col2,
        |       '2' as col3,
        |       'True' as col4
        |union all
        |select 'jerry' as col1,
        |       null as col2,
        |       '3' as col3,
        |       'True' as col4
        |""".stripMargin).createOrReplaceTempView("temp")

    spark.sql(
      """
        |select * from temp
        |""".stripMargin).show(false)

    println("----------------------------------")

    spark.sql(
      """
        |SELECT map('col1', col1, 'col2', col2, 'col3', col3, 'col4', col4) AS map,
        |       row_number() OVER (ORDER BY col1) AS mk
        |from temp
        |""".stripMargin).createOrReplaceTempView("mapTable")

    spark.sql(
      """
        |select * from mapTable
        |""".stripMargin).show(false)

    println("------------------------------")

    spark.sql(
      """
        |SELECT mk, k, v
        |FROM mapTable
        |         LATERAL VIEW EXPLODE(map) mapTemp AS k, v
        |""".stripMargin).createOrReplaceTempView("explodeTable")

    spark.sql(
      """
        |select * from explodeTable order by mk
        |""".stripMargin).show(false)

    println("-----------------------")

    spark.sql(
      s"""
        |SELECT mk,
        |       k,
        |       v,
        |       CASE WHEN k in ('col1', 'col2', 'col3') AND v IS NULL THEN 'notNull'
        |            WHEN k = 'col4' AND v NOT IN ("True", "False") AND v is NOT NULL THEN 'not in Range'
        |            ELSE NULL END AS FilterType,
        |       CASE WHEN k in ('col1', 'col2', 'col3') AND v IS NULL THEN k
        |            WHEN k = 'col4' AND v NOT IN ("True", "False") AND v is NOT NULL THEN concat(k, "不在规定范围内(true, false)")
        |            ELSE NULL END AS col
        |from explodeTable
        |""".stripMargin).createOrReplaceTempView("result")

    spark.sql(
      """
        |select * from result order by mk
        |""".stripMargin).show(false)

    println("-----------------")

    spark.sql(
      """
        |SELECT mk, collect_set(FilterType) AS FilterType, collect_list(col) AS col
        |from result
        |GROUP BY mk
        |ORDER BY mk
        |;
        |""".stripMargin).createOrReplaceTempView("res1")

    spark.sql(
      """
        |SELECT t2.map,
        |       t1.FilterType,
        |       t1.col
        |from res1 AS t1
        |    INNER JOIN
        |    mapTable AS t2 ON t1.mk = t2.mk
        |""".stripMargin).createOrReplaceTempView("res2")





    spark.sql(
      """
        |SELECT map['col1'] AS col1,
        |       map['col2'] AS col2,
        |       map['col3'] AS col3,
        |       map['col4'] AS col4,
        |       FilterType,
        |       col
        |FROM res2
        |""".stripMargin).show(false)






  }

}
