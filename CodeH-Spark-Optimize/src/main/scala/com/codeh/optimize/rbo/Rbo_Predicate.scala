package com.codeh.optimize.rbo

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className Rbo_Predicate
 * @author jinhua.xu
 * @date 2021/12/9 13:47
 * @description RBO(Rule-Based Optimization)之谓词下推性能优化：谓词下推是将过滤条件的谓词都尽可能的提前执行，减少下游处理的数据量
 * @version 1.0
 *
 *          1：对于left join使用on过滤只会下推右表，在where后过滤两表都会下推
 *          2：列剪裁：就是扫描数据源的时候，只读取哪些与查询相关的字段
 *          3：常量替换：自动将sql中的表达式为固定值的先提前转为固定值
 */
object Rbo_Predicate {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Rbo_Predicate")
      .setMaster("local[*]")

    val spark: SparkSession = InitUtils.getSparkSession(conf)

    spark.sql("use sparktuning")

    /*
    println("=======================================Inner on 左表=======================================")
    val innerStr1 =
    """
        |select
        |  l.courseid,
        |  l.coursename,
        |  r.courseid,
        |  r.coursename
        |from sale_course l join course_shopping_cart r
        |  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn
        |  and l.courseid<2
      """.stripMargin
    sparkSession.sql(innerStr1).show()
    sparkSession.sql(innerStr1).explain(mode = "extended")

    println("=======================================Inner where 左表=======================================")
    val innerStr2 =
      """
        |select
        |  l.courseid,
        |  l.coursename,
        |  r.courseid,
        |  r.coursename
        |from sale_course l join course_shopping_cart r
        |  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn
        |where l.courseid<2
      """.stripMargin
    sparkSession.sql(innerStr2).show()
    sparkSession.sql(innerStr2).explain(mode = "extended")
*/

    println("=======================================left on 左表=======================================")
    val leftStr1 =
      """
        |select
        |  l.courseid,
        |  l.coursename,
        |  r.courseid,
        |  r.coursename
        |from sale_course l left join course_shopping_cart r
        |  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn
        |  and l.courseid<2
      """.stripMargin
    spark.sql(leftStr1).show()
    spark.sql(leftStr1).explain(mode = "extended")

    println("=======================================left where 左表=======================================")
    val leftStr2 =
      """
        |select
        |  l.courseid,
        |  l.coursename,
        |  r.courseid,
        |  r.coursename
        |from sale_course l left join course_shopping_cart r
        |  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn
        |where l.courseid<2
      """.stripMargin
    spark.sql(leftStr2).show()
    spark.sql(leftStr2).explain(mode = "extended")


    println("=======================================left on 右表=======================================")
    val leftStr3 =
      """
        |select
        |  l.courseid,
        |  l.coursename,
        |  r.courseid,
        |  r.coursename
        |from sale_course l left join course_shopping_cart r
        |  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn
        |  and r.courseid<2
      """.stripMargin
    spark.sql(leftStr3).show()
    spark.sql(leftStr3).explain(mode = "extended")

    println("=======================================left where 右表=======================================")
    val leftStr4 =
      """
        |select
        |  l.courseid,
        |  l.coursename,
        |  r.courseid,
        |  r.coursename
        |from sale_course l left join course_shopping_cart r
        |  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn
        |where r.courseid<2 + 3
      """.stripMargin
    spark.sql(leftStr4).show()
    spark.sql(leftStr4).explain(mode = "extended")
  }
}
