package com.codeh.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @className Memory_par_02
 * @author jinhua.xu
 * @date 2021/4/2 14:33
 * @description 并行度
 * @version 1.0
 */
object Spark02_Memory_Par {
  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("memory-par")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    /**
     * TODO Spark - 从内存中创建RDD
     *    RDD中的分区的数量就是并行度，设定并行度，其实就是设定分区数量
     *
     *  1.makeRDD的第一个参数：数据源
     *  2.makeRDD的第二个参数：默认并行度（分区数量） parallelize
     *
     *   override def defaultParallelism(): Int =
     *      scheduler.conf.getInt("spark.default.parallelism", totalCores)
     *
     *  并行度默认会从spark配置信息中获取 spark.default.parallelism
     *  如过获取不到指定参数，会采用默认值totalCore（机器的总核数）
     *  机器总核数 = 当前环境中可用核数
     *
     *  local = 单核 =》1
     *  local[*] => 最大核数
     */
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)  // TODO 内存中的集合数据按照平均分的方式进行分区处理
    rdd.saveAsTextFile("output")  // 处理后的数据保存为文本文件

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),4)
    rdd1.saveAsTextFile("output1")

    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),3) // TODO 内存中的集合数据如果不能平均分，会采用分区算法进行区分数据
    rdd2.saveAsTextFile("output2")

    // 3.关闭连接
    sc.stop()
  }
}
