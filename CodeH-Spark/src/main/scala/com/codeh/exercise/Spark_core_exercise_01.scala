package com.codeh.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @className Spark_core_exercise_01
 * @author jinhua.xu
 * @date 2021/7/28 14:32
 * @description spark core练习
 * @version 1.0
 */
object Spark_core_exercise_01 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_core_exercise_01").setMaster("local[*]")

    //    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\score.csv")

    val mapRDD: RDD[((Int, String), Int)] = rdd.map(
      line => {
        val data: Array[String] = line.split(",")
        ((data(0).toInt, data(2)), data(4).toInt)
      }
    )

    val reduceRDD: RDD[((Int, String), Int)] = mapRDD.reduceByKey(_ + _)

    val t1: RDD[(String, (Int, Int))] = reduceRDD.map {
      case ((id, class_name), total) => {
        (class_name, (id, total))
      }
    }

    val groupRDD: RDD[(String, Iterable[(Int, Int)])] = t1.groupByKey()

    val res: RDD[(String, List[(Int, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(1)
      }
    )

    res.collect().foreach(println)
//    reduceRDD.collect().foreach(println)

  }
}

//case class Student(id: Int, student_name: String, class_name: String, subject_name: String, score: Int, teacher_name: String)
