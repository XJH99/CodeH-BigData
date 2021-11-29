package com.codeh.optimize.explain

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className ExplainDemo
 * @author jinhua.xu
 * @date 2021/11/26 18:40
 * @description explain 功能测试
 * @version 1.0
 */
object ExplainDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("explain")
      .setMaster("local[*]")  // todo 打包放到集群执行需要注释掉

    val spark: SparkSession = InitUtils.getSparkSession(conf)

    spark.sql("use sparktuning")

    val sql: String =
      """
        |select
        |  sc.courseid,
        |  sc.coursename,
        |  sum(sellmoney) as totalsell
        |from sale_course sc join course_shopping_cart csc
        |  on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn
        |group by sc.courseid,sc.coursename
        |""".stripMargin

    // 重点使用
    println("=============================================explain()-只展示物理执行计划=====================================")
    spark.sql(sql).show()

    println("=============================================explain(mode=\"simple\")-只展示物理执行计划=====================================")
    //spark.sql(sql).explain(mode = "simple")

    // 重点
    println("=============================================explain(mode=\"extended\")-展示逻辑和物理执行计划=====================================")
    //spark.sql(sql).explain(mode = "extended")

    println("=============================================explain(mode=\"codegen\")-展示可执行Java代码=====================================")
    //spark.sql(sql).explain(mode = "codegen")

    // 重点
    println("=============================================explain(mode=\"formatted\")-展示格式化的物理执行计划=====================================")
    //spark.sql(sql).explain(mode = "formatted")

  }
}
