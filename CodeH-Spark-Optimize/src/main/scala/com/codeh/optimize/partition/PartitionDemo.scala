package com.codeh.optimize.partition

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @className PartitionDemo
 * @author jinhua.xu
 * @date 2021/12/2 13:27
 * @description 默认的并行度是200
 * @version 1.0
 */
object PartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PartitionDemo")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") // 禁用广播join
      .setMaster("local[*]")

    val spark: SparkSession = InitUtils.getSparkSession(conf)

    val df1: DataFrame = spark.sql("select * from sparktuning.sale_course")
    val df2: DataFrame = spark.sql("select * from sparktuning.course_pay")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime") // 更改列名
    val df3: DataFrame = spark.sql("select * from sparktuning.course_shopping_cart")
      .drop("coursename") // 删除指定列
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    df1.join(df3, Seq("courseid", "dt", "dn"), "right")
      .join(df2, Seq("orderid", "dt", "dn"), "left")
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
      .write.mode(SaveMode.Overwrite).saveAsTable("sparktuning.salecourse_detail")

    spark.stop()
  }
}
