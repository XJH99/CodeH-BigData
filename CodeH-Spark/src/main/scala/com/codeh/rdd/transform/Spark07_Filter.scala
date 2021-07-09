package com.codeh.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark07_Filter
 * @author jinhua.xu
 * @date 2021/4/2 15:04
 * @description 过滤算子
 * @version 1.0
 */
object Spark07_Filter {
  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("File-RDD-Filter")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    /**
     * TODO 过滤
     * 将数据根据指定的规则进行筛选过滤
     */
    val dataRDD = sc.makeRDD(List(1,2,3,4,5,6),3)

    val rdd = dataRDD.filter(item => {
      item % 2 == 0
    })

    println(rdd.collect().mkString(","))

    // 3.关闭连接
    sc.stop()
  }

}
