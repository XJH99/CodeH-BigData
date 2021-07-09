package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark05_FlatMap
 * @author jinhua.xu
 * @date 2021/4/2 14:50
 * @description 扁平化算子操作
 * @version 1.0
 */
object Spark05_FlatMap {
  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File-RDD-FlatMap")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\word.txt")

    /**
     * 对字符串进行切割，然后切割后的数据打平
     */
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    println(flatRDD.collect().mkString(" "))

    // 3.关闭连接
    sc.stop()
  }

}
