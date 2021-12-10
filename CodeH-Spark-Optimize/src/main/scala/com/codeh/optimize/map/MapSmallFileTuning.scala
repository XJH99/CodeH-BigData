package com.codeh.optimize.map

import com.codeh.optimize.utils.InitUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @className MapSmallFileTuning
 * @author jinhua.xu
 * @date 2021/12/9 18:11
 * @description map端读取小文件优化
 * @version 1.0
 */
object MapSmallFileTuning {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MapSmallFileTuning")
      .set("spark.files.openCostInBytes", "7194304") //文件开销默认4m
      .set("spark.sql.files.maxPartitionBytes", "128MB") //默认128M
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉
    val sparkSession: SparkSession = InitUtils.getSparkSession(sparkConf)


    sparkSession.sql("select * from sparktuning.course_shopping_cart")
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("sparktuning.test")

    sparkSession.stop()
  }
}
