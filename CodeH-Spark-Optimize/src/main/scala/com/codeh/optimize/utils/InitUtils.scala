package com.codeh.optimize.utils

import java.util.Random

import com.codeh.optimize.bean.{School, Student}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

/**
 * @className InitUtils
 * @author jinhua.xu
 * @date 2021/11/24 17:31
 * @description 数据初始化工具类
 * @version 1.0
 */
object InitUtils {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf: SparkConf = new SparkConf().setAppName("init_data")
    .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉

    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    // 查看dfs的端口号hdfs getconf -confKey fs.default.name
    // 配置hdfs的访问路径
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://192.168.214.150:9820")

    initHiveTable(spark)

    spark.stop()

  }

  /**
   * 数据初始化导入
   * @param spark spark环境对象
   */
  def initHiveTable(spark: SparkSession): Unit = {
    spark.read.json("/sparkdata/coursepay.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_pay")

    spark.read.json("/sparkdata/salecourse.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.sale_course")

    spark.read.json("/sparkdata/courseshoppingcart.log")
      .write.partitionBy("dt", "dn")
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.course_shopping_cart")
  }

  def saveData(spark: SparkSession): Unit = {
    import spark.implicits._

    val ds_student: Dataset[Student] = spark.range(1000000).mapPartitions(partitions => {
      val random: Random = new Random()
      partitions.map(item => Student(item, "name" + item, random.nextInt(100), random.nextInt(100)))
    })

    ds_student.write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_student")

    val ds_school: Dataset[School] = spark.range(1000000).mapPartitions(partitions => {
      val random = new Random()
      partitions.map(item => School(item, "school" + item, random.nextInt(100)))
    })

    ds_school.write.partitionBy("partition")
      .mode(SaveMode.Append)
      .saveAsTable("sparktuning.test_student")

  }
}
