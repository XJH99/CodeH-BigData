package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark03_Map
 * @author jinhua.xu
 * @date 2021/4/2 14:39
 * @description map转换算子操作
 * @version 1.0
 */
object Spark04_Map {
  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("file-operator")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 转换算子：
     *  能够将旧的RDD通过方法转换得到一个新的RDD
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val result1: RDD[Int] = rdd.map(_ * 2)

    /**
     * mapPartitions
     * 以分区为单位进行计算，和map算子很像
     * 区别在于map算子是一个一个执行，而mapPartitions一个分区一个分区执行，类似于批处理,但是容易出现内存溢出
     *
     * map方法是全量数据操作，不能丢失数据
     * mapPartitions 一次性获取分区所有数据，那么可以执行迭代器集合的所有操作，过滤，max，min
     */
    val result2: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator // 注意返回一个迭代器类型
    })

    /**
     * 获取第二个分区的数据，获取的分区索引从0开始
     */
    val result3: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) iter
      else Nil.iterator // 不满足条件返回空迭代
    })

    println(result1.collect().mkString(" "))
    println(result2.collect().mkString(" "))
    println(result3.collect().mkString(" "))

    // 3.关闭连接
    sc.stop()

  }

}
