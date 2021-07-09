package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark16_GroupByKey
 * @author jinhua.xu
 * @date 2021/4/19 15:31
 * @description 对key进行分组
 * @version 1.0
 */
object Spark16_GroupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark16_GroupByKey")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("hello", 1), ("java", 1), ("spark", 1), ("java", 1), ("scala", 2)
    ))

    // TODO groupByKey: 根据数据的key进行分组
    /**
     * 调用groupByKey后，返回数据的类型是元组
     * 元组的第一个元素表示用于分组的key
     * 元组的第二个元素表示分组后相同key的value集合
     */
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    // 统计单词的个数
    val wordToCount: RDD[(String, Int)] = groupRDD.map {
      case (word, iter) => {
        (word, iter.size)
      }
    }

    println(wordToCount.collect().mkString(","))

    // 3.关闭连接
    sc.stop()

  }

}
