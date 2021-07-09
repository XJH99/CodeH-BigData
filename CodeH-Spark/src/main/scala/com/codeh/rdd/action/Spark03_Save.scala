package com.codeh.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark03_Save
 * @author jinhua.xu
 * @date 2021/4/19 16:55
 * @description 结果数据写出保存
 * @version 1.0
 */
object Spark03_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_Save")

    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4, 5, 6, 7, 8
    ))

    //rdd.saveAsTextFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\output")
    //rdd.saveAsObjectFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\output1")
    rdd.map((_, 1)).saveAsSequenceFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\output2")

    sc.stop()
  }

}
