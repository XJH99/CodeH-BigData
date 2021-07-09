package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark20_Join
 * @author jinhua.xu
 * @date 2021/4/19 15:54
 * @description join操作
 * @version 1.0
 */
object Spark20_Join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark20_Join")

    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 3), ("c", 2)
    ))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("c", 1), ("a", 3), ("b", 5)
    ))

    /**
     * join 方法可以将两个rdd中相同的key的value连接在一起
     * join方法的性能不太高，能不用尽量不要用
     */
    val result: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    // 左外连接的使用
    //val result: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    println(result.collect().mkString(","))

    sc.stop()
  }

}
