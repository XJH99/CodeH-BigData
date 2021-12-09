package com.codeh.optimize.broadcast

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.broadcast

/**
 * @className SMBJoin
 * @author jinhua.xu
 * @date 2021/12/9 15:46
 * @description SMBJoin优化：sort merge bucket分桶表进行join处理，分桶的目的就是把大表化成小表，相同key的数据在同一个桶中之后再进行join操作
 * @version 1.0
 */
object SMBJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SMBJoinTuning")
      .set("spark.sql.shuffle.partitions", "20")
    val sparkSession: SparkSession = InitUtils.getSparkSession(sparkConf)
    useSMBJoin(sparkSession)
  }

  def useSMBJoin( sparkSession: SparkSession ) = {
    //查询出三张表 并进行join 插入到最终表中
    val saleCourse = sparkSession.sql("select *from sparktuning.sale_course")
    val coursePay = sparkSession.sql("select * from sparktuning.course_pay_cluster")
      .withColumnRenamed("discount", "pay_discount")
      .withColumnRenamed("createtime", "pay_createtime")
    val courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart_cluster")
      .drop("coursename")
      .withColumnRenamed("discount", "cart_discount")
      .withColumnRenamed("createtime", "cart_createtime")

    val tmpdata = courseShoppingCart.join(coursePay, Seq("orderid"), "left")
    val result = broadcast(saleCourse).join(tmpdata, Seq("courseid"), "right")
    result
      .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "sparktuning.sale_course.dt", "sparktuning.sale_course.dn")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.salecourse_detail_2")
  }
}
