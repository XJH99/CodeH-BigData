package com.codeh.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark_Accumulator
 * @author jinhua.xu
 * @date 2021/4/23 10:55
 * @description 累加器：把executor端变量信息聚合到driver端
 * @version 1.0
 */
object Spark_Accumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Accumulator")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9))

    // 创建累加器对象
    val sum: LongAccumulator = sc.longAccumulator("sum")

    rdd.foreach(
      // 遍历元素进行累加操作
      num => {
        sum.add(num)
      }
    )

    println("sum = " + sum.value)

    sc.stop()
  }

}
