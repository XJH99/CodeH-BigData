package com.codeh.sink;

import com.codeh.bean.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Sink_Mysql
 * @date 2021/8/12 16:38
 * @description 自定义mysql输出源
 */
public class Flink_Sink_Mysql {
    public static void main(String[] args) {

    }

    /**
     * RichSinkFunction<IN>
     * IN:输入的参数类型
     */
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        Connection conn = null;
        PreparedStatement insertPrep = null;
        PreparedStatement updatePrep = null;

        /**
         * 初始化操作
         *
         * @param parameters 配置参数
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc：mysql://192.168.214.136:3306/test", "root", "root");

            // 创建编译器，使用占位符
            insertPrep = conn.prepareStatement("INSERT INTO sensor_tmp(id, temp) VALUES (?, ?)");
            updatePrep = conn.prepareStatement("UPDATE sensor_tmp SET temp = ? WHERE id = ?");
        }

        /**
         * 调用连接，执行sql
         * @param value 传入的数据
         * @param context 上下文对象
         * @throws Exception
         */
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
           updatePrep.setDouble(1, value.getTemperature());
           updatePrep.setString(2, value.getId());
           updatePrep.execute();

           // 如果更新语句没有更新，就插入操作
           if (updatePrep.getUpdateCount() == 0) {
               insertPrep.setString(1, value.getId());
               insertPrep.setDouble(2, value.getTemperature());
               insertPrep.execute();
           }
        }

        /**
         * 关闭资源
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            insertPrep.close();
            updatePrep.close();
            conn.close();
        }
    }
}
