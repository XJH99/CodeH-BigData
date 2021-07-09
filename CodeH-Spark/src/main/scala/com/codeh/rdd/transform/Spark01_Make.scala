package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Make_01
 * @author jinhua.xu
 * @date 2021/4/2 14:27
 * @description rdd 的创建方式
 * @version 1.0
 */
object Spark01_Make {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("make").setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    /**
     * rdd的三种常用创建方式
     *
     * makeRDD的底层还是使用了parallelize来进行创建
     */
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(5, 6, 7, 8))
    val rdd3: RDD[String] = sc.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\word.txt")

    println(rdd1.collect().mkString(" "))
    println(rdd2.collect().mkString(" "))
    println(rdd3.collect().mkString(" "))

    sc.stop()
  }

}
