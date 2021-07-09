package com.codeh.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark10_Repartition
 * @author jinhua.xu
 * @date 2021/4/19 14:56
 * @description 可以扩大分区，会进行shuffle操作
 * @version 1.0
 */
object Spark10_Repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark10_Repartition")

    val sc = new SparkContext(sparkConf)

    val dataRDD = sc.makeRDD(List(1, 1, 1, 6, 6, 6), 2)

    /**
     * 扩大分区：
     * coalesce主要目的是缩减分区，扩大分区时没有啥效果
     * 因为要将数据进行扩大分区，那么必须打乱数据进行重新组合，必须使用shuffle
     * 其实repartition的底层还是使用了coalesce(numPartitions: Int, shuffle: Boolean = false)
     */
    val rdd = dataRDD.repartition(6)

    val rdd1 = dataRDD.coalesce(6, true)

    rdd.saveAsTextFile("output")
    rdd1.saveAsTextFile("output1")

    // 3.关闭连接
    sc.stop()
  }
}
