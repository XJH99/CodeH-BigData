package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @className Spark13_PartitionBy
 * @author jinhua.xu
 * @date 2021/4/19 15:16
 * @description 对key的数据进行分区操作
 * @version 1.0
 */
object Spark13_PartitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark13_PartitionBy")

    val sc = new SparkContext(sparkConf)

    // K-V数据类型的操作
    val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    // TODO Spark中很多的方法是基于key进行操作，所以数据格式应该为键值对
    /**
     * 使用了隐式转换
     * partitionBy方法取自于PairRDDFunctions类
     * RDD的伴生对象中提供了隐式函数可以将RDD转换为PairRDDFunctions类
     *
     * partitionBy:根据指定的规则对数据进行分区,参数为分区器对象
     * 分区器对象：HashPartitioner & RangePartitioner
     *
     * HashPartitioner分区规则是将当前数据的key进行取余操作
     * HashPartitioner是spark默认的分区器
     */
    val rdd1: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(2))

    rdd1.saveAsTextFile("output")
    // 3.关闭连接
    sc.stop()
  }

}
