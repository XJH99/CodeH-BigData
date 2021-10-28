package com.codeh.test

import org.apache.spark.sql.SparkSession

/**
 * @className SparkSQL_Test02
 * @author jinhua.xu
 * @date 2021/10/22 15:12
 * @description 时区测试
 * @version 1.0
 */
object SparkSQL_Test02 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("demo01")
      .config("spark.sql.session.timeZone", "UTC+08")
      .getOrCreate()

    import spark.implicits._

    spark.sql(
      """
        |SELECT pow(UNIX_TIMESTAMP('20211020', 'yyyyMMdd'), 1) AS value,
        |       to_utc_timestamp('2016-08-31', 'Asia/Shanghai') AS ds
        |""".stripMargin).show()

    spark.stop()
  }

}
