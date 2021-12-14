package com.codeh.optimize.excel

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @className Spark_Excel_Demo
 * @author jinhua.xu
 * @date 2021/12/7 11:05
 * @description 测试spark解析excel功能
 * @version 1.0
 */
object Spark_Excel_Demo {
  var excel_name: String = "D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark-Optimize\\data\\cdp_label_def.xlsx"
  var sheet_name: String = "Sheet1"
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Spark_Excel_Demo")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    println("Read excel")
//    val ds: Dataset[TagDef] = spark.read
//      .format("com.crealytics.spark.excel")
//      .option("location", excel_name)
//      .option("sheetName", sheet_name)
//      .option("workbookPassword", "pass")
//      .option("treatEmptyValuesAsNulls", "true")
//      .option("useHeader", "true")
//      .option("inferSchema", "true")
//      .option("addColorColumns", "False")
//      .load().withColumn("backup", lit(null)).as[TagDef]

    val ds: Dataset[TagDef] = spark.read
      .format("com.crealytics.spark.excel")
      .option("workbookPassword", "pass")
      .option("inferSchema", "true")
      .option("dataAddress", "'Sheet1'!A1")
      .option("header", "true")
      .load(excel_name).withColumn("backup", lit(null)).as[TagDef]

    // 筛选出标签计算的group
    val group_ds: Dataset[TagDef] = ds.where("group_name in ('G1', 'G2', 'G3', 'G4')")

    // todo 需要做sql的explain



    println(group_ds.count())

//    ds.show(false)
//    println(ds.count())

    spark.stop()
  }
}

case class TagDef(label_id: String,
                  label_name: String,
                  parent_label_id: String,
                  value: String,
                  enable: String,
                  group_name: String,
                  source_table: String,
                  source_table_sql: String,
                  logic_sql: String,
                  backup: String)
