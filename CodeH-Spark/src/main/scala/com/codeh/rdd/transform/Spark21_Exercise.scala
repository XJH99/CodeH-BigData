package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark21_Exercise
 * @author jinhua.xu
 * @date 2021/4/19 16:13
 * @description 统计每个省份，广告点击的前三
 * @version 1.0
 */
object Spark21_Exercise {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark21_Exercise")

    val sc: SparkContext = new SparkContext(sparkConf)

    // 1.读取数据
    val rdd: RDD[String] = sc.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\agent.log")

    // 2.对数据进行转换，转成想要的格式
    val mapRDD: RDD[(String, Int)] = rdd.map(
      data => {
        val arr: Array[String] = data.split(" ")
        (arr(1) + "-" + arr(4), 1)
      }
    )

    // 3.对key进行分组统计数量
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    // 4.对分组统计后的数据再转化格式 (省份, (广告, 点击数))
    val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
      case (str, count) => {
        val arr: Array[String] = str.split("-")
        (arr(0), (arr(1), count))
      }
    }

    // 5.对key进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

    // 6.对分组后的value值排序取前三
    val resRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    )

    // 7.打印结果
//    println(resRDD.collect().mkString(","))
    resRDD.collect().foreach(println)

    // 8.关闭资源
    sc.stop()

  }

}
