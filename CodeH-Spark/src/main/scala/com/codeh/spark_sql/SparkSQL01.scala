package com.codeh.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @className SparkSQL01
 * @author jinhua.xu
 * @date 2021/8/2 11:40
 * @description spark_sql 官网案例
 * @version 1.0
 */
object SparkSQL01 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_01")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("spark sql basic example")
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Spark\\input\\user.json")

    df.printSchema()

    /**
     * root
     * |-- age: long (nullable = true)
     * |-- name: string (nullable = true)
     */

    df.select("name").show()

    /**
     * +--------+
     * |    name|
     * +--------+
     * |zhangsan|
     * |    lisi|
     * |  wangwu|
     * +--------+
     */

    df.select($"name", $"age" + 1).show()

    /**
     * +--------+---------+
     * |    name|(age + 1)|
     * +--------+---------+
     * |zhangsan|       21|
     * |    lisi|       31|
     * |  wangwu|       41|
     * +--------+---------+
     */

    df.filter($"age" > 21).show()

    /**
     * +---+------+
     * |age|  name|
     * +---+------+
     * | 30|  lisi|
     * | 40|wangwu|
     * +---+------+
     */

    df.groupBy($"age").count().show()

    /**
     * +---+-----+
     * |age|count|
     * +---+-----+
     * | 30|    1|
     * | 20|    1|
     * | 40|    1|
     * +---+-----+
     */

    // 创建临时视图进行查询
    //    df.createOrReplaceTempView("people")
    //
    //    spark.sql("select * from people").show()

    // 创建全局视图,全局视图查表表名需要加上global_temp
    df.createGlobalTempView("people")

    spark.sql("select * from global_temp.people").show()
    // 需要关闭资源
    spark.stop()
  }

}
