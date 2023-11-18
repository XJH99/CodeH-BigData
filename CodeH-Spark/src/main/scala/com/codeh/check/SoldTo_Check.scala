package com.codeh.check

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.collection.mutable.ListBuffer

/**
 * @author jinhua.xu
 * @date 2023/5/25 20:18
 * @version 1.0
 * @describe
 */
object SoldTo_Check {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SoldTo_Check")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // val checkNull = (column: String) => col(column).isNull
    // val addFields = (typeValue: String, colNameValue: String) => (lit(typeValue).alias("FilterType"), lit(colNameValue).alias("col"))

    val salesSkill: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load("/Users/xujinhua/IdeaProjects/CodeH-BigData/CodeH-Spark/input/SalesSkill.csv")
      .select("Sales_Skill_UUID", "Sales_Skill_ID")

    salesSkill.show(false)

    salesSkill.createOrReplaceTempView("salesSkill")

    val last_date = getDate(-1)

    val soldTo = spark.read.format("csv")
      .option("header", true)
      .load("/Users/xujinhua/IdeaProjects/CodeH-BigData/CodeH-Spark/input/SoldTo.csv")
      .where(s"modifiedon = '2023-05-24'")

    soldTo.show(false)

    soldTo.createOrReplaceTempView("soldTo")

    val soldto_event_check: DataFrame = spark.sql(
      """
        |select t1.*,
        |       t2.Sales_Skill_ID as SalesSkill
        |from soldTo as t1
        |       left join
        |     salesSkill as t2 on t1.dfc_sales_skillid = t2.Sales_Skill_UUID
        |""".stripMargin)

    soldto_event_check.show(false)

    soldto_event_check.createOrReplaceTempView("SalesToTable")

    println("soldto_event_check schema:")

    soldto_event_check.printSchema()


    //val allCol = List("modifiedon", "dfc_sales_skillid", "dfc_city_id", "dfc_countryid", "dfc_customer_en_name", "dfc_billing_block",
    //  "dfc_invoic_type", "dfc_bank_account")

    val allCol = "'modifiedon', 'dfc_sales_skillid', 'dfc_city_id', 'dfc_countryid', 'dfc_customer_en_name', 'dfc_billing_block', 'dfc_invoic_type', 'dfc_bank_account'," +
      "'dfc_billing_block_reason', 'dfc_group_customer', 'dfc_ebs_groupid', 'dfc_payement_periodid', 'dfs_recommmend_account_periodid'"


    val notNullCol = List("dfc_sales_skillid", "dfc_city_id", "dfc_countryid", "dfc_customer_en_name", "dfc_invoic_type")

    val notNull_notInRange = List("dfc_billing_block")

    val notNullMore = "'dfc_bank_account'"

    println("----------------")

    spark.sql(
      s"""
         |SELECT MAP('modifiedon', modifiedon,
         |           'dfc_sales_skillid', dfc_sales_skillid,
         |           'dfc_city_id', dfc_city_id,
         |           'dfc_countryid', dfc_countryid,
         |           'dfc_customer_en_name', dfc_customer_en_name,
         |           'dfc_billing_block', dfc_billing_block,
         |           'dfc_invoic_type', dfc_invoic_type,
         |           'dfc_bank_account', dfc_bank_account,
         |           'dfc_billing_block_reason', dfc_billing_block_reason,
         |           'dfc_group_customer', dfc_group_customer,
         |           'dfc_ebs_groupid', dfc_ebs_groupid) AS cols_map,
         |       ROW_NUMBER() OVER (ORDER BY modifiedon)   AS row_num
         |FROM SalesToTable
         |""".stripMargin).createOrReplaceTempView("SalesToTableRes1")

    spark.sql(
      s"""
        |SELECT row_num,
        |       col_name,
        |       col_value
        |FROM SalesToTableRes1
        |         LATERAL VIEW EXPLODE(cols_map) tmpTable AS col_name, col_value
        |""".stripMargin).createOrReplaceTempView("SalesToTableRes2")

    spark.sql(
      s"""
        |SELECT t1.row_num,
        |       t1.col_name,
        |       t1.col_value,
        |       CASE
        |           WHEN t1.col_name IN
        |                ('dfc_sales_skillid', 'dfc_city_id', 'dfc_countryid', 'dfc_customer_en_name', 'dfc_invoic_type',
        |                 'dfc_billing_block') AND
        |                t1.col_value IS NULL THEN 'notNull'
        |           WHEN t1.col_name IN ('dfc_bank_account') AND t1.col_value IS NULL AND t2.col_name = 'dfc_invoic_type' AND
        |                t2.col_value = '10' THEN 'notNull'
        |           WHEN t1.col_name = 'dfc_billing_block_reason' AND t1.col_value IS NULL AND
        |                t3.col_name = 'dfc_billing_block' AND t3.col_value = 'true' THEN 'notNull'
        |           WHEN t1.col_name = 'dfc_central_block_reason' AND t1.col_value IS NULL AND
        |                t4.col_name = 'dfc_central_block' AND t4.col_value = 'true' THEN 'notNull'
        |           WHEN t1.col_name = 'dfc_ebs_groupid' AND t1.col_value IS NULL AND
        |                t5.col_name = 'dfc_group_customer' AND t5.col_value = 'true' THEN 'notNull'
        |
        |           WHEN t1.col_name IN ('dfc_billing_block') AND t1.col_value IS NOT NULL AND
        |                t1.col_value NOT IN ('true', 'false') THEN 'not in Range'
        |           ELSE NULL
        |           END AS FilterType,
        |       CASE
        |           WHEN t1.col_name IN
        |                ('dfc_sales_skillid', 'dfc_city_id', 'dfc_countryid', 'dfc_customer_en_name', 'dfc_invoic_type') AND
        |                t1.col_value IS NULL THEN t1.col_name
        |           WHEN t1.col_name IN ('dfc_bank_account') AND t1.col_value IS NULL AND t2.col_name = 'dfc_invoic_type' AND
        |                t2.col_value = '10' THEN CONCAT('dfc_invoic_type为10,', t1.col_name, '不为空')
        |           WHEN t1.col_name = 'dfc_billing_block_reason' AND t1.col_value IS NULL AND
        |                t3.col_name = 'dfc_billing_block' AND t3.col_value = 'true' THEN CONCAT('dfc_billing_block = true,', t1.col_name, '不为空')
        |           WHEN t1.col_name = 'dfc_central_block_reason' AND t1.col_value IS NULL AND
        |                t4.col_name = 'dfc_central_block' AND t4.col_value = 'true' THEN CONCAT('dfc_central_block = true,', t1.col_name, '不为空')
        |           WHEN t1.col_name = 'dfc_ebs_groupid' AND t1.col_value IS NULL AND
        |                t5.col_name = 'dfc_group_customer' AND t5.col_value = 'true' THEN CONCAT('dfc_group_customer = true,', t1.col_name, '不为空')
        |
        |           WHEN t1.col_name IN ('dfc_billing_block') AND t1.col_value IS NOT NULL AND
        |                t1.col_value NOT IN ('true', 'false') THEN CONCAT(t1.col_name, '不在规定范围内(true,false)')
        |           ELSE NULL
        |           END AS col
        |FROM SalesToTableRes2 AS t1
        |         LEFT JOIN
        |     (SELECT * FROM SalesToTableRes2 WHERE col_name = 'dfc_invoic_type' AND col_value = '10') AS t2 ON t1.row_num = t2.row_num
        |         LEFT JOIN
        |     (SELECT * FROM SalesToTableRes2 WHERE col_name = 'dfc_billing_block' AND col_value = 'true') AS t3 ON t1.row_num = t3.row_num
        |        LEFT JOIN
        |    (SELECT * FROM SalesToTableRes2 WHERE col_name = 'dfc_central_block' AND col_value = 'true') AS t4 ON t1.row_num = t4.row_num
        |        LEFT JOIN
        |     (SELECT * FROM SalesToTableRes2 WHERE col_name = 'dfc_group_customer' AND col_value = 'true') AS t5 ON t1.row_num = t5.row_num
        |""".stripMargin).createOrReplaceTempView("SalesToTableRes3")

    spark.sql(
      """
        |select * from SalesToTableRes3 order by row_num
        |""".stripMargin).show(50, false)


    spark.sql(
      s"""
        |SELECT row_num, COLLECT_SET(FilterType) AS FilterType, COLLECT_LIST(col) AS col
        |FROM SalesToTableRes3
        |GROUP BY row_num
        |""".stripMargin).createOrReplaceTempView("SalesToTableRes4")

    spark.sql(
      s"""
        |SELECT t2.cols_map,
        |       t1.FilterType,
        |       t1.col
        |from SalesToTableRes4 AS t1
        |    INNER JOIN
        |     SalesToTableRes1 AS t2 ON t1.row_num = t2.row_num
        |""".stripMargin).createOrReplaceTempView("SalesToTableRes5")

    spark.sql(
      s"""
        |SELECT cols_map['modifiedon'] AS modifiedon,
        |       cols_map['dfc_sales_skillid'] AS dfc_sales_skillid,
        |       cols_map['dfc_city_id'] AS dfc_city_id,
        |       cols_map['dfc_countryid'] AS dfc_countryid,
        |       cols_map['dfc_customer_en_name'] AS dfc_customer_en_name,
        |       cols_map['dfc_billing_block'] AS dfc_billing_block,
        |       cols_map['dfc_invoic_type'] AS dfc_invoic_type,
        |       cols_map['dfc_bank_account'] AS dfc_bank_account,
        |       cols_map['dfc_billing_block_reason'] AS dfc_billing_block_reason,
        |       cols_map['dfc_group_customer'] AS dfc_group_customer,
        |       cols_map['dfc_ebs_groupid'] AS dfc_ebs_groupid,
        |       FilterType,
        |       col
        |FROM SalesToTableRes5
        |""".stripMargin).show(false)



    //    val newSchema = StructType(soldto_event_check.schema ++ Seq(
    //      StructField("FilterType", StringType, true),
    //      StructField("col", StringType, true)
    //    ))
    //
    //    val newRDD: RDD[Row] = soldto_event_check.rdd.map(r => {
    //      val list: ListBuffer[Row] = ListBuffer[Row]()
    //      var row: Row = null
    //
    //      for (col <- allCol) {
    //        println(col + "----" + r.getAs(col))
    //        var buffer: Seq[Any] = Row.unapplySeq(r).get
    ////        println(buffer.mkString(","))
    //        if (notNullCol.contains(col) && r.getAs[String](col) == null) {
    //          buffer = buffer ++ Seq("notNull", col)
    //          row = new GenericRowWithSchema(buffer.toArray, newSchema)
    //          list.append(row)
    //        }
    //      }
    //
    //      list
    //    }).flatMap(fl => fl)
    //
    //    spark.createDataFrame(newRDD, newSchema).show(false)

  }


  def getDate(addDate: Int = 0): String = {
    val time = new Date().getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    sdf.format(time + (86400000 * addDate))
  }

  def isNumber(str: String, len: Int): Boolean = {
    if (str.length == len && str.matches("\\d+")) true else false
  }

  def isNumber(str: String): Boolean = {
    if (str.matches("\\d+")) true else false
  }
}
