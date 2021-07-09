package com.codeh.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark04_Foreach
 * @author jinhua.xu
 * @date 2021/4/19 17:02
 * @description 遍历
 * @version 1.0
 */
object Spark04_Foreach {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark04_Foreach")

    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 7, 8))

    // TODO foreach 算子，算子的逻辑代码是在分布式计算节点Executor执行的
    // foreach 算子可以将循环在不同的计算节点完成
    // 算子之外的代码是在Driver端执行
    rdd.foreach(println)

    // TODO foreach 方法，集合的方法中的代码是在当前节点中执行的
    // foreach 方法是在当前节点的内存中完成数据的循环
    rdd.collect().foreach(println)

    sc.stop()
  }

}
