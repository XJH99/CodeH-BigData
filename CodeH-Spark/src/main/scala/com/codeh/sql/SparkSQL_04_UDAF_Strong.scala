package com.codeh.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @className SparkSQL_04_UDAF_Strong
 * @author jinhua.xu
 * @date 2021/4/23 15:23
 * @description 自定义强类型UDFA函数
 * @version 1.0
 */
object SparkSQL_04_UDAF_Strong {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_04_UDAF_Strong")

    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 2.创建聚合函数对象
    val udaf: MyAvgStrongFunction = new MyAvgStrongFunction()

    // 3.将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")

    val frame: DataFrame = spark.read.json("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\user.json")

    val dataDS: Dataset[UserBean] = frame.as[UserBean]

    dataDS.select(avgCol).show()

    // 5.释放资源
    spark.stop()
  }
}

case class UserBean(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Long)

/**
 * 声明自定义强类型聚合函数
 *  1.继承Aggregator函数
 *  2.实现方法
 */
class MyAvgStrongFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  // 初始化缓冲区
  override def zero: AvgBuffer = AvgBuffer(0, 0)

  // 聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 合并多个节点数据
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // 固定写法，默认的编解码器
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  // 固定写法
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
