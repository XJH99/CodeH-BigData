package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark08_Distinct
 * @author jinhua.xu
 * @date 2021/4/2 15:05
 * @description 去重算子操作
 * @version 1.0
 */
object Spark08_Distinct {
  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File-RDD-Distinct")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1,3,3,6,6))

    val distinctRDD: RDD[Int] = dataRDD.distinct(2)

    println(distinctRDD.collect().mkString(" "))

    sc.stop()


  }

}
