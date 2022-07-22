package com.codeh.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 * @className CleanData
 * @author jinhua.xu
 * @date 2022/2/23 18:27
 * @description 黑名单用户标签数据过滤
 * @version 1.0
 *          +------------------+----------------------------------------------+--------+--------+
 *          |one_id            |tags                                          |biz_date|etl_date|
 *          +------------------+----------------------------------------------+--------+--------+
 *          |BOS210325400019595|[T00002 -> , T00004 -> , T00003 -> ]          |20220215|20220216|
 *          |BOS210325400043187|[]                                            |20220215|20220216|
 *          |BOS210325400090829|[T00014 -> 1.6281792E9, T00013 -> 1.6281792E9]|20220215|20220216|
 *          +------------------+----------------------------------------------+--------+--------+
 */
object CleanData {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("init_data")
      .setMaster("local[*]") //TODO 要打包提交集群执行，注释掉

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val map: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    map.put("T00004", "")
    map.put("T00002", "")
    map.put("T00013", "1.6281792E9")
    map.put("T00014", "1.6281792E9")
    map.put("T00003", "")

    val map1: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    map1.put("T00013", "1.6281792E9")
    map1.put("T00014", "1.6281792E9")

    val tagRDD: RDD[(String, mutable.HashMap[String, String], String, String)] = sc.makeRDD(List(
      ("BOS210325400019595", map, "20220215", "20220216"),
      ("BOS210325400043187", map1, "20220215", "20220216"),
      ("BOS210325400090829", map1, "20220215", "20220216")
    ))

    println(tagRDD.collect().mkString(",") + "\n")

    val arr: Array[(String, String)] = sc.makeRDD(List(
      ("BOS210325400019595", "T00013"),
      ("BOS210325400019595", "T00014"),
      ("BOS210325400043187", "T00013"),
      ("BOS210325400043187", "T00014")
    )).collect()

    val broadcast: Broadcast[Array[(String, String)]] = sc.broadcast(arr)

    val resultRDD: RDD[(String, mutable.HashMap[String, String], String, String)] = tagRDD.map {
      case (one_id, tags, biz_date, etl_date) => {
        for ((key, tagId) <- broadcast.value) {
          if (one_id == key && tags.contains(tagId)) {
            // 当one_id相同时并且tagId为黑名单的标签去除标签数据
            tags -= tagId
          }
        }
        (one_id, tags, biz_date, etl_date)
      }
    }

    //val frame: DataFrame = resultRDD.toDF("one_id", "tags", "biz_date", "etl_date").where($"one_id".isin("BOS210325400019595", "BOS210325400043187"))
    val frame: DataFrame = resultRDD.toDF("one_id", "tags", "biz_date", "etl_date")

    frame.show(false)

    spark.stop()
  }

}
