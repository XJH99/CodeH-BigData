package com.codeh.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark01_Action
 * @author jinhua.xu
 * @date 2021/4/19 16:49
 * @description 行动算子
 * @version 1.0
 */
object Spark01_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_Action")

    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 所谓的行动算子，其实不会再产生新的RDD，而是出发作业的执行
     * 行动算子执行后，会获取到作业的执行结果，
     * 转换算子不会触发作业的执行，只是功能的扩展和包装
     */
    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4
    ))

    // Spark的行动算子执行时，会产生Job对象，然后提交这个Job对象
    val ints: Array[Int] = rdd.collect()

    ints.foreach(println)

    sc.stop()
  }

}
