package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark15_ReduceByKey
 * @author jinhua.xu
 * @date 2021/4/19 15:29
 * @description 对相同key里面的数据进行计算
 * @version 1.0
 */
object Spark15_ReduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark15_ReduceByKey")

    val sc = new SparkContext(sparkConf)

    // TODO reduceByKey: 根据数据的key进行分组，然后对value进行聚合
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("hello", 1), ("java", 1), ("spark", 1), ("java", 1)
    ))

    /**
     * reduceByKey 中第二个参数表示聚合后的分区数量
     */
    val rdd1: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    println(rdd1.collect().mkString(","))

    // 3.关闭连接
    sc.stop()
  }

}
