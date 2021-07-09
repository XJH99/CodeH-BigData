package com.codeh.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
 * @className SparkSQL_03_UDF
 * @author jinhua.xu
 * @date 2021/4/23 15:08
 * @description 弱类型自定义UDAF功能实现
 * @version 1.0
 */
object SparkSQL_03_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_03_UDAF")

    // 1.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 2.创建聚合函数对象
    val udaf: MyAvgFunction = new MyAvgFunction()

    // 3.注册聚合函数
    spark.udf.register("avgAge", udaf)

    // 使用聚合函数
    val frame: DataFrame = spark.read.json("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\user.json")

    frame.createOrReplaceTempView("people")

    spark.sql("select avgAge(age) from people").show()

    // 5.释放资源
    spark.stop()
  }

}

/**
 * 自定义聚合函数
 *  1.继承UserDefinedAggregateFunction spark3.0已弃用
 *  2.实现方法
 */
class MyAvgFunction extends UserDefinedAggregateFunction {
  // 输入数据的数据结构
  override def inputSchema: StructType = new StructType().add("age", LongType)

  // 计算时的数据结构
  override def bufferSchema: StructType = new StructType().add("sum", LongType).add("count", LongType)

  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算之前缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = input.getLong(0) + buffer.getLong(0)  //传入值求和
    buffer(1) = buffer.getLong(1) + 1 // 数量加一
  }

  // 多个节点缓冲区数据累加
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 返回结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}
