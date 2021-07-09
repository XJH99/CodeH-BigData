package com.codeh.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className WordCount
 * @author jinhua.xu
 * @date 2021/4/2 14:11
 * @description TODO
 * @version 1.0
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建spark运行环境配置对象
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")

    // 创建spark上下文对象
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\word.txt")

    // 算子转换，得到WordCount的结果
    val result: Array[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()

    print(result.mkString(","))

    // 关闭连接
    sc.stop()

  }

}
