package com.codeh.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * @className SparkSQL_05_MySQL
 * @author jinhua.xu
 * @date 2021/4/23 16:16
 * @description Spark通过jdbc方式连接mysql数据库
 * @version 1.0
 */
object SparkSQL_05_MySQL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_05_MySQL")

    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 2.创建配置对象，并设置配置信息
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

//    val frame: DataFrame = read(spark, prop)
//    frame.show()

    write(spark, prop)

    spark.stop()
  }

  /**
   * 读取mysql中的数据
   *
   * @param spark SparkSession
   * @param prop  Properties
   */
  def read(spark: SparkSession, prop: Properties): DataFrame = {
     spark.read.jdbc("jdbc:mysql://hadoop103:3306/test", "mysql_table", prop)
  }

  /**
   * 将数据写入到mysql中
   * @param spark SparkSession
   * @param prop Properties
   */
  def write(spark: SparkSession, prop: Properties): Unit = {
    import spark.implicits._
    val ds: Dataset[User] = spark.sparkContext.makeRDD(List(User(20, "codeH"))).toDS()
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop103:3306/test", "tb1", prop)
    println("写入jdbc完成")
  }

  case class User(id: Int, name: String)

}
