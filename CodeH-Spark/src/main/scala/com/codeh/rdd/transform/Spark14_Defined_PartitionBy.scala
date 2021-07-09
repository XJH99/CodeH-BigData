package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @className Spark14_Defined_PartitionBy
 * @author jinhua.xu
 * @date 2021/4/19 15:23
 * @description 自定义分区器功能
 * @version 1.0
 */
object Spark14_Defined_PartitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark14_Defined_PartitionBy")

    val sc = new SparkContext(sparkConf)

    // 自定义分区器
    val rdd = sc.makeRDD(List(("nba", "消息1"), ("cba", "消息2"), ("wcba", "消息3"), ("nba", "消息4"), ("nba", "消息5"), ("cba", "消息6")))

    val rdd1: RDD[(String, String)] = rdd.partitionBy(new MyPartition(2))

    val rdd2: RDD[((String, String), Int)] = rdd1.mapPartitionsWithIndex((index, datas) => {
      datas.map(data => (data, index))
    })

    println(rdd2.collect().mkString(","))
    // 3.关闭连接
    sc.stop()
  }

}

class MyPartition(num: Int) extends Partitioner {

  override def numPartitions: Int = num

  /**
   *
   * @param key 决定数据在那个分区中进行处理
   * @return 表示分区编号
   */
  override def getPartition(key: Any): Int = {
    key match {
      case "nba" => 0
      case _ => 1
    }
  }

}
