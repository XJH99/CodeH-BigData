package com.codeh.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @className Spark06_Group
 * @author jinhua.xu
 * @date 2021/4/2 14:55
 * @description 分组操作
 * @version 1.0
 */
object Spark06_Group {

  def main(args: Array[String]): Unit = {
    // 1.创建spark运行环境配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("group")

    // 2.创建spark文件上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

//    val sum: Double = rdd.mapPartitions(iter => {
//      List(iter.max).iterator
//    }).sum()

    /**
     * TODO 分组
     * group by 方法可以根据指定的规则进行分组，指定的规则的返回值就是分组的key
     * group 方法的返回值为元组
     *    元组中的第一个元素，表示分组的key
     *    元组中的第二个元素，表示相同key的数据形成的可迭代集合
     * group by方法执行完毕后，会将数据进行分组操作，但是分区是不会改变的；不同组的数据会打乱在不同的分区中
     *
     * groupBy方法会导致数据不均匀，产生shuffle操作，如果想改变分区，可以传递参数
     */
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(data => {
      data % 2
    })

    groupRDD.collect().foreach {
      case (key, list) => {
        println("key: " + key + " " + "list: " + list.mkString(","))
      }
    }

    sc.stop()
  }

}
