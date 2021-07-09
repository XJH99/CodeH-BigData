package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark18_CombineByKey
 * @author jinhua.xu
 * @date 2021/4/19 15:44
 * @description TODO
 * @version 1.0
 */
object Spark18_CombineByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark18_CombineByKey")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)
    ), 2)

    /**
     * TODO 计算相同key的平均值
     *
     * 88 =》 (88,1) +91 = (179,2) +95 => (274,3)
     * 计算时需要将value的格式发生改变，只需要第一个v发生改变结构即可
     * 如果计算时发现相同的key的value不符合计算规则的格式的话，那么选择combineByKey
     *
     *
     * TODO combineByKey方法可以传递三个参数
     * 第一个参数表示的就是将计算的第一个值转换结构
     * 第二个参数表示分区内的计算规则
     * 第三个参数表示分区间的计算规则
     */
    val result: RDD[(String, (Int, Int))] = rdd.combineByKey( // key一致的情况
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    result.map {
      case (key, (total, count)) => {
        (key, total / count)
      }
    }.collect().foreach(println)


    // 3.关闭连接
    sc.stop()
  }

}
