package com.codeh.optimize.broadcast

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className AutoBroadcastJoin
 * @author jinhua.xu
 * @date 2021/12/9 15:14
 * @description 自动广播join性能优化, 适用于小表与大表的join
 * @version 1.0
 */
object AutoBroadcastJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BroadcastJoinTuning")
      // spark.sql.autoBroadcastJoinThreshold默认值就是10m
      .set("spark.sql.autoBroadcastJoinThreshold", "10m")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtils.getSparkSession(sparkConf)

    val sqlstr =
      """
        |select
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin

    sparkSession.sql("use sparktuning;")
    sparkSession.sql(sqlstr).show()

    sparkSession.stop()
  }
}
