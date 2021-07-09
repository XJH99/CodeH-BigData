package com.codeh.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @className Partition_03
 * @author jinhua.xu
 * @date 2021/4/2 14:36
 * @description 切片分区原理
 * @version 1.0
 */
object Spark03_Partition {
  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("file-RDD")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    /**
     * TODO Spark - 从磁盘中创建RDD
     *
     *  1.Spark读取文件采用的是Hadoop的读取规则
     *   文件切片规则：以字节的方式来进行切片
     *   数据读取规则：以行为单位来读取
     *
     *  2.问题
     *    TODO 文件到底切分为了几片
     *    假设文件字节数（10），预计切片数量（2）
     *    10 / 2 = 5 byte
     *    totalSize = 10
     *    goalSize = totalSize / numSplits = 10 / 2 = 5
     *    所谓的最小分区数，取决于总的字节数是否能够整除分区数并且剩余的字节达到一个比率
     *    实际产生的分区数量可能大于最小分区数
     *
     *    TODO 分区的数据如何存储
     *    分区数据是以行为单位读取的，而不是字节
     *
     * hadoop分区是以文件为单位进行划分的；读取文件不能跨越文件
     *
     */
    val fileRdd: RDD[String] = sc.textFile("input/w.txt",2)
    fileRdd.saveAsTextFile("output")

    // 3.关闭连接
    sc.stop()
  }

}
