package com.codeh.optimize.car

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @className Car_lottery
 * @author jinhua.xu
 * @date 2022/6/13 14:10
 * @description TODO
 * @version 1.0
 */
object Car_lottery {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("Car")
      // TODO: Shuffle读写缓冲区大小调优，默认是32KB和48M
      .set("spark.shuffle.file.buffer", "48KB")
      .set("spark.reducer.maxSizeInFlight", "72M")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // todo: spark AQE自动分区合并
    spark.sql("set spark.sql.adaptive.enabled=true")
    spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=true")
    spark.sql("set spark.sql.adaptive.advisoryPartitionSizeInBytes=200M")
    spark.sql("set spark.sql.adaptive.coalescePartitions.minPartitionNum=6")

    // TODO: 调整广播变量的阈值，让小表进行广播优化
    spark.sql("set spark.sql.autoBroadcastJoinThreshold=20M")

    val rootPath: String = ""
    val hdfs_path_apply: String = s"${rootPath}/apply"
    val hdfs_path_lucky: String = s"${rootPath}/lucky"

    val applyNumDF: DataFrame = spark.read.parquet(hdfs_path_apply)
    val luckyDogsDF: DataFrame = spark.read.parquet(hdfs_path_lucky)

    //todo：做缓存优化
    applyNumDF.cache()
    luckyDogsDF.cache()

    // 统计申请者和中签者的数量
    applyNumDF.count()
    luckyDogsDF.count()

    // 实际摇号人数的数据量
    val applyDistinctDF: Dataset[Row] = applyNumDF.select("batchNum", "carNum").distinct()

    //todo: 缓存优化，这块的优化效果比较好
    applyDistinctDF.cache()
    applyDistinctDF.count()

    // 摇号次数以及对应得人数
    val result02_01: Dataset[Row] = applyDistinctDF.groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    // 摇号批次得中签者的数量
    val result02_02: Dataset[Row] = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum")).agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis")).agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    // 中签率的变化趋势
    val apply_denominator: DataFrame = applyDistinctDF.groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("denominator"))

    val lucky_molecule: DataFrame = luckyDogsDF.groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("molecule"))

    val result03: Dataset[Row] = apply_denominator
      .join(lucky_molecule, Seq("batchNum"), "inner")
      .withColumn("ratio", round(col("molecule") / col("denominator"), 5))
      .orderBy("batchNum")

    // 筛选出2018年的中签数据，并按照批次统计中签人数
    val lucky_molecule_2018 = luckyDogsDF
      .filter(col("batchNum").like("2018%"))
      .groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("molecule"))

    // 通过与筛选出的中签数据按照批次做关联，计算每期的中签率
    val result04 = apply_denominator
      .join(lucky_molecule_2018, Seq("batchNum"), "inner")
      .withColumn("ratio", round(col("molecule")/col("denominator"), 5))
      .orderBy("batchNum")

    // 不同倍率下的中签人数
    val result05_01 = applyNumDF
      .join(luckyDogsDF.filter(col("batchNum") >= "201601")
        .select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("batchNum"),col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum")
      .agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier")
      .agg(count(lit(1)).alias("cnt"))
      .orderBy("multiplier")


    // Step01: 过滤出2016-2019申请者数据，统计出每个申请者在每一期内的倍率，并在所有批次中选取最大的倍率作为申请者的最终倍率，最终算出各个倍率下的申请人数
    val apply_multiplier_2016_2019 = applyNumDF
      .filter(col("batchNum") >= "201601")
      .groupBy(col("batchNum"), col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum")
      .agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier")
      .agg(count(lit(1)).alias("apply_cnt"))

    // Step02: 将各个倍率下的申请人数与各个倍率下的中签人数左关联，并求出各个倍率下的中签率
    val result05_02 = apply_multiplier_2016_2019
      .join(result05_01.withColumnRenamed("cnt", "lucy_cnt"), Seq("multiplier"), "left")
      .na.fill(0)
      .withColumn("ratio", round(col("lucy_cnt")/col("apply_cnt"), 5))
      .orderBy("multiplier")




  }
}
