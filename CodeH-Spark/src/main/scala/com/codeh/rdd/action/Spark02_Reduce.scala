package com.codeh.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark02_Reduce
 * @author jinhua.xu
 * @date 2021/4/19 16:51
 * @description TODO
 * @version 1.0
 */
object Spark02_Reduce {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Reduce")

    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // TODO Reduce
    // 简化，规约
    val i: Int = rdd.reduce(_ + _)

    // TODO collect
    // collect方法会将所有分区计算的结果拉取到当前节点的内存中，可能会出现内存溢出
    val array: Array[Int] = rdd.collect()

    // TODO count 返回RDD中元素的个数
    val con: Long = rdd.count()

    // TODO first 返回RDD中的第一个元素
    val first: Int = rdd.first()

    // TODO take 取出前几个元素
    val ints: Array[Int] = rdd.take(3)

    // TODO takeOrdered 先排序再进行取前几个元素
    val ints1: Array[Int] = rdd.takeOrdered(3)

    // TODO sum 求和  注意返回类型是double
    val d: Double = rdd.sum()

    // TODO aggregate
    /**
     * aggregateByKey: 初始值只参与到分区内计算
     * aggregate：初始值分区内计算会参与，同时分区间计算也会参与
     */
    val i1: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(i1)

    // TODO countByKey 通过key来进行汇总
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 1), ("a", 1)
    ))

    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()

    println(stringToLong)

    // TODO countByValue 通过value来进行汇总
    val rdd2: RDD[String] = sc.makeRDD(List(
      "a", "a", "a", "b", "c", "hello"
    ))

    val stringToLong1: collection.Map[String, Long] = rdd2.countByValue()

    println(stringToLong1)

    sc.stop()


  }

}
