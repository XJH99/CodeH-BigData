package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark12_Tow_Value
 * @author jinhua.xu
 * @date 2021/4/19 15:10
 * @description 双值操作
 * @version 1.0
 */
object Spark12_Tow_Value {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark12_Tow_Value")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    // TODO 并集,数据合并,分区也会合并
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    rdd3.saveAsTextFile("output1")
    //println(rdd3.collect().mkString(","))

    // TODO 交集，保留最大分区数，数据被打乱重组，shuffle机制
    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    rdd4.saveAsTextFile("output2")
    //println(rdd4.collect().mkString(","))

    // TODO 差集,已当前rdd的分区为主，所有分区数量等于当前rdd的分区数量
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    rdd5.saveAsTextFile("output3")
    //println(rdd5.collect().mkString(","))

    // TODO 拉链，分区数不变
    /**
     * 如果两个rdd的分区数不一致时，会报错
     * 如果两个rdd分区相同，但是分区内数量不一致，也是会报错
     */
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    rdd6.saveAsTextFile("output4")
    //println(rdd6.collect().mkString(","))

    sc.stop()

  }

}
