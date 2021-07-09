package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark17_AggregateByKey
 * @author jinhua.xu
 * @date 2021/4/19 15:38
 * @description
 * @version 1.0
 */
object Spark17_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark17_AggregateByKey")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3), ("b", 4), ("c", 5), ("c", 6)
    ), 2)

    /**
     * TODO: 取出每个分区内相同的key的最大值然后分区相加
     * aggregateByKey算子是函数的柯里化，存在两个参数列表
     *
     *  1.第一个参数列表中的参数表示初始值
     *  2.第二个参数列表中含有两个参数
     *    2.1 第一个参数表示分区内的计算规则
     *    2.2 第二个参数表示分区间的计算规则
     */
    val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(3)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    /**
     * 如果分区内计算规则和分区间计算规则相同，那么可以将aggregateByKey简化为另外一个方法foldByKey
     */
    val result: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)

    resultRDD.collect().foreach(println)

    println(result.collect().mkString(","))

    // 3.关闭连接
    sc.stop()
  }

}
