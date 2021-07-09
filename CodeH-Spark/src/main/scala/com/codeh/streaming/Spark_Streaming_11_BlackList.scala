package com.codeh.streaming

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat

import com.codeh.utils.JDBCUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

/**
 * @className Spark_Streaming_11_BlackList
 * @author jinhua.xu
 * @date 2021/4/29 11:26
 * @description 广告黑名单实现功能
 * @version 1.0
 */
object Spark_Streaming_11_BlackList {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Streaming_11_BlackList")

    // 第二个参数表示批量处理的周期
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 定义kafka的一些配置
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.214.136:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "hypers",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )


    // 1.kafka工具类获取消费者订阅的数据
    val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("spark_streaming_kafka"), kafkaPara)
    )

    // 2.将kafka中读取的数据封装成样例类模式
    val adClkData: DStream[AdClkData] = kafkaData.map(
      data => {
        val values: Array[String] = data.value().split(" ")
        AdClkData(values(0), values(1), values(2), values(3), values(4))
      }
    )

    // 3.进行黑名单数据过滤，并简单wordCount统计
    val ds: DStream[((String, String, String), Int)] = adClkData.transform(
      rdd => {
        // 存放黑名单用户数据
        val blackList = ListBuffer[String]()

        // TODO 周期性的获取黑名单数据
        val conn: Connection = JDBCUtils.getConnection
        val statement: PreparedStatement = conn.prepareStatement("SELECT userid FROM black_list")
        // 获取到黑名单的结果集
        val res: ResultSet = statement.executeQuery()
        if (res.next()) {
          blackList.append(res.getString(1))
        }
        res.close()
        statement.close()
        conn.close()

        // TODO 过滤黑名单的用户数据
        val filterRDD: RDD[AdClkData] = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )

        // TODO 统计过滤后的数据
        val reduceRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          data => {
            val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            val day: String = sdf.format(new java.util.Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)

        reduceRDD
      }
    )

    // 4.
    ds.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          item => {
            val conn: Connection = JDBCUtils.getConnection
            item.foreach {
              case ((day, user, ad), count) => {
                println(s"${day} ${user} ${ad} ${count}")
                if (count >= 30) {
                  // TODO 点击数量达到阈值，将该用户拉到黑名单中
                  val sql =
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid = ?
                            """.stripMargin
                  JDBCUtils.executeUpdate(conn, sql, Array(user, user))
                  conn.close()
                } else {
                  // TODO 数量没有到阈值，需要将当天的点击数更新
                  val sql =
                    """
                      | select
                      |     *
                      | from user_ad_count
                      | where dt = ? and userid = ? and adid = ?
                            """.stripMargin
                  val bool: Boolean = JDBCUtils.isExist(conn, sql, Array(day, user, ad))

                  if (bool) {
                    // TODO 如果存在数据，那么更新
                    val updateSQL =
                      """
                        | update user_ad_count
                        | set count = count + ?
                        | where dt = ? and userid = ? and adid = ?
                                     """.stripMargin
                    JDBCUtils.executeUpdate(conn, updateSQL, Array(count, day, user, ad))

                    // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单
                    val selectSQL =
                      """
                        |select
                        |    *
                        |from user_ad_count
                        |where dt = ? and userid = ? and adid = ? and count >= 30
                                     """.stripMargin
                    val flag: Boolean = JDBCUtils.isExist(conn, selectSQL, Array(day, user, ad))

                    if (flag) {
                      // TODO 超过，加入到黑名单中
                      val insertSQL =
                        """
                          |insert into black_list (userid) values (?)
                          |on DUPLICATE KEY
                          |UPDATE userid = ?
                                 """.stripMargin
                      JDBCUtils.executeUpdate(conn, insertSQL, Array(user, user))
                    }
                  } else {
                    // TODO 不存在数据，插入
                    val sql4 =
                      """
                        | insert into user_ad_count ( dt, userid, adid, count ) values ( ?, ?, ?, ? )
                                           """.stripMargin
                    JDBCUtils.executeUpdate(conn, sql4, Array(day, user, ad, count))
                  }
                }
              }
            }
            conn.close()
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

case class AdClkData(ts: String, area: String, city: String, user: String, ad: String)
