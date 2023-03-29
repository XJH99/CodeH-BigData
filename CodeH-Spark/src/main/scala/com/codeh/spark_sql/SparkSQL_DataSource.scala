package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedWriter, File, FileOutputStream, FileWriter}

/**
 * @className SparkSQL_DataSource
 * @author jinhua.xu
 * @date 2021/12/24 15:13
 * @description spark读取各种数据源操作
 * @version 1.0
 */
object SparkSQL_DataSource extends Serializable {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkSQL_DataSource").setMaster("local[*]")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.registerKryoClasses(Array(classOf[java.io.BufferedWriter]))
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._
    // spark读取数据时，可以忽略损坏的文件
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    // spark读取数据时，忽略丢失的文件
    spark.sql("set spark.sql.files.ignoreMissingFilesDataFrame=true")

    val ds = readCSV(spark).as[Score]


    try {
      val file = new File("CodeH-Spark/output/score.csv")

      val file_parent = new File(file.getParent)

      if (!file_parent.exists()) {
        file_parent.mkdir()
        println("父目录创建成功")
      }

      if (!file.exists()) {
        file.createNewFile()
      }

      val bw = new BufferedWriter(new FileWriter(file))


      ds.collect().foreach(data => {
        val sb = new StringBuilder
        val str = sb.append(data.id).append(",")
          .append(data.name).append(",")
          .append(data.class_name).append(",")
          .append(data.subject).append(",")
          .append(data.score).append(",")
          .append(data.teacher).toString()
        println(str)
        bw.write(str.toCharArray)
        bw.newLine()
        bw.flush()
      })

      bw.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }


    //    println(ds.count())

    spark.stop()
  }

  /**
   * 读取csv文件
   *
   * @param spark
   * @return
   */
  def readCSV(spark: SparkSession): DataFrame = {
    val frame: DataFrame = spark.read.format("csv")
      .option("sep", ",") // 数据分隔符
      .option("inferSchema", "true")
      .option("header", "true") // 是否包含表头
      .load("CodeH-Spark/input/score.csv")

    frame
  }

  def readJson(spark: SparkSession): DataFrame = {
    val frame: DataFrame = spark.read.format("json").load("CodeH-Spark/input/user.json")
    frame
  }
}

case class Score(id: String,
                 name: String,
                 class_name: String,
                 subject: String,
                 score: String,
                 teacher: String)
