package com.codeh.optimize.partition

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @className PartitionOptimize
 * @author jinhua.xu
 * @date 2021/12/2 15:02
 * @description
 * @version 1.0
 */
object PartitionOptimize {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PartitionOptimize")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") // 禁用广播join
      .set("spark.sql.shuffle.partitions", "20") // 调整task的并行度，最好为cpu core的2~3倍
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
