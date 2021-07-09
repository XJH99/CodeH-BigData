package com.codeh.rdd.accumulator

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

/**
 * @className Spark_MyAccumulator
 * @author jinhua.xu
 * @date 2021/4/23 11:01
 * @description 自定义累加器功能
 * @version 1.0
 */
object Spark_MyAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_MyAccumulator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hadoop", "hive", "spark", "java", "scala", "base"))

    // 1.创建累加器对象
    val acc: MyAccumulator = new MyAccumulator

    // 2.注册累加器
    sc.register(acc, "accDemo")

    // 3.累加操作
    rdd.foreach(
      str => {
        acc.add(str)
      }
    )

    // 4.打印输出
    println("result: " + acc.value)

    sc.stop()

  }

}

/**
 * 自定义累加器：
 *    1.继承AccumulatorV2
 *    2.实现抽象方法
 */
class MyAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  // 累加器初始化状态
  override def isZero: Boolean = list.isEmpty

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new MyAccumulator

  // 重置累加器对象
  override def reset(): Unit = list.clear()

  // 累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  // 合并数据
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = list.addAll(other.value)

  // 返回累加器对象
  override def value: util.ArrayList[String] = list
}
