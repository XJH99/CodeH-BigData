package com.codeh.optimize.cbo

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className CBO_Statics
 * @author jinhua.xu
 * @date 2021/12/9 14:38
 * @description CBO(cost_based Optimization)优化：根据实际数据分布和组织情况，评估每个计划的执行代价，从而选择代价最小的执行计划
 * @version 1.0
 */
object CBO_Statics {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CBOTunning")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val spark: SparkSession = InitUtils.getSparkSession(sparkConf)

    AnalyzeTableAndColumn(spark, "sparktuning.sale_course", "courseid,dt,dn")
    AnalyzeTableAndColumn(spark, "sparktuning.course_shopping_cart", "courseid,orderid,dt,dn")
    AnalyzeTableAndColumn(spark, "sparktuning.course_pay", "orderid,dt,dn")


  }

  def AnalyzeTableAndColumn(sparkSession: SparkSession, tableName: String, columnListStr: String): Unit = {
    //TODO 查看 表级别 信息
    println("=========================================查看" + tableName + "表级别 信息========================================")
    sparkSession.sql("DESC FORMATTED " + tableName).show(100)
    //TODO 统计 表级别 信息
    println("=========================================统计 " + tableName + "表级别 信息========================================")
    sparkSession.sql("ANALYZE TABLE " + tableName + " COMPUTE STATISTICS").show()
    //TODO 再查看 表级别 信息
    println("======================================查看统计后 " + tableName + "表级别 信息======================================")
    sparkSession.sql("DESC FORMATTED " + tableName).show(100)


    //TODO 查看 列级别 信息
    println("=========================================查看 " + tableName + "表的" + columnListStr + "列级别 信息========================================")
    val columns: Array[String] = columnListStr.split(",")
    for (column <- columns) {
      sparkSession.sql("DESC FORMATTED " + tableName + " " + column).show()
    }
    //TODO 统计 列级别 信息
    println("=========================================统计 " + tableName + "表的" + columnListStr + "列级别 信息========================================")
    sparkSession.sql(
      s"""
         |ANALYZE TABLE ${tableName}
         |COMPUTE STATISTICS
         |FOR COLUMNS $columnListStr
      """.stripMargin).show()
    //TODO 再查看 列级别 信息
    println("======================================查看统计后 " + tableName + "表的" + columnListStr + "列级别 信息======================================")
    for (column <- columns) {
      sparkSession.sql("DESC FORMATTED " + tableName + " " + column).show()
    }
  }

}
