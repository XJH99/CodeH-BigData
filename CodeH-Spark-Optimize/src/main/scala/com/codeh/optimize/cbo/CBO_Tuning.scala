package com.codeh.optimize.cbo

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className CBO_Tuning
 * @author jinhua.xu
 * @date 2021/12/9 14:56
 * @description CBO优化使用
 * @version 1.0
 */
object CBO_Tuning {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CBOTuning")
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.joinReorder.enabled", "true")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉

    val spark: SparkSession = InitUtils.getSparkSession(sparkConf)

    val sqlstr =
      """
        |select
        |  csc.courseid,
        |  sum(cp.paymoney) as coursepay
        |from course_shopping_cart csc,course_pay cp
        |where csc.orderid=cp.orderid
        |and cp.orderid ='odid-0'
        |group by csc.courseid
      """.stripMargin

    spark.sql("use sparktuning;")
    spark.sql(sqlstr).show()
  }
}
