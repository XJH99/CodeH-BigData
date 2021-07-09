package com.codeh.rdd.dependencies

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @className Spark_Dependencies
 * @author jinhua.xu
 * @date 2021/4/22 17:37
 * @description spark中的宽依赖与窄依赖的处理
 * @version 1.0
 */
object Spark_Dependencies {
  def main(args: Array[String]): Unit = {
    // TODO Spark 依赖关系

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Dependencies")

    val sc = new SparkContext(sparkConf)

    /**
     * RDD 窄依赖表示每一个父RDD的Partition最多被子RDD的一个Partition使用，窄依赖我们形象的比喻为独生子女
     *
     * RDD 宽依赖表示同一个父RDD的Partition被多个子RDD的Partition依赖，会引起Shuffle，总结：宽依赖我们形象的比喻为超生
     */
    val fileRDD: RDD[String] = sc.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\word.txt")
    println(fileRDD.dependencies)
    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)
    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)

    resultRDD.collect()

    sc.stop()
  }

}
