package com.codeh.optimize.data_skew

import java.util.Random

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @className SkewAggregation
 * @author jinhua.xu
 * @date 2021/12/9 16:44
 * @description 使用随机key双重聚合的方式来解决大数据量key的数据倾斜问题
 * @version 1.0
 */
object SkewAggregation {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("SkewAggregation")
      .set("spark.sql.shuffle.partitions", "36")
      .setMaster("local[*]")

    val spark: SparkSession = InitUtils.getSparkSession(sparkConf)

    // todo 注册udf函数
    spark.udf.register("random_prefix", (value: Int, num: Int) => randomPrefixUDF(value, num))
    spark.udf.register("remove_random_prefix", ( value: String ) => removeRandomPrefixUDF(value))

    val sql1 =
      """
        |select
        |  courseid,
        |  sum(course_sell) totalSell
        |from
        |  (
        |    select
        |      remove_random_prefix(random_courseid) courseid,
        |      course_sell
        |    from
        |      (
        |        select
        |          random_courseid,
        |          sum(sellmoney) course_sell
        |        from
        |          (
        |            select
        |              random_prefix(courseid, 6) random_courseid,
        |              sellmoney
        |            from
        |              sparktuning.course_shopping_cart
        |          ) t1
        |        group by random_courseid
        |      ) t2
        |  ) t3
        |group by
        |  courseid
      """.stripMargin


    val sql2=
      """
        |select
        |  courseid,
        |  sum(sellmoney)
        |from sparktuning.course_shopping_cart
        |group by courseid
      """.stripMargin

    spark.sql(sql1).show(100)

    spark.stop()
  }

  def randomPrefixUDF( value: Int, num: Int ): String = {
    new Random().nextInt(num).toString + "_" + value
  }

  def removeRandomPrefixUDF( value: String ): String = {
    value.toString.split("_")(1)
  }
}
