package com.codeh.utils

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

/**
 * @className JDBCUtils
 * @author jinhua.xu
 * @date 2021/4/29 10:29
 * @description 连接mysql的工具类
 * @version 1.0
 */
object JDBCUtils {

  var dataSource: DataSource = init()

  /**
   * 初始化数据源连接池
   *
   * @return DataSource
   */
  def init(): DataSource = {
    val properties: Properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://192.168.214.136:3306/test?useUnicode=true&characterEncoding=UTF-8")
    properties.setProperty("username", "root")
    properties.setProperty("password", "codeH0000/")
    properties.setProperty("maxActive", "50") // 最大连接数
    DruidDataSourceFactory.createDataSource(properties)
  }

  /**
   * 获取Mysql的连接对象
   *
   * @return Connection
   */
  def getConnection: Connection = {
    dataSource.getConnection
  }

  /**
   * 执行SQL语句,单条数据插入
   * @param connection
   * @param sql
   * @param params
   * @return
   */
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
   * 判断数据是否在库中存在
   * @param connection
   * @param sql
   * @param params
   * @return
   */
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

}
