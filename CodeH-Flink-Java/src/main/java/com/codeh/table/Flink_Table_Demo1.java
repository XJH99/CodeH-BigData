package com.codeh.table;

import com.codeh.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Table_Demo1
 * @date 2021/8/29 14:21
 * @description Flink table / sql 基础demo
 */
public class Flink_Table_Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> source = env.readTextFile("CodeH-Flink-Java/src/main/resources/Sensor.txt");

        DataStream<SensorReading> mapStream = source.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 1.创建table环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        // 2.flink table api
        Table table = tableEnvironment.fromDataStream(mapStream);

        Table tableResult = table.select("id, temperature")
                .where("id = 'sensor_1'");

        // 3.flink sql api
        tableEnvironment.createTemporaryView("sensor", mapStream);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";

        Table sqlResult = tableEnvironment.sqlQuery(sql);

        // 4.写出数据
        tableEnvironment.toAppendStream(tableResult, Row.class).print("table");
        tableEnvironment.toAppendStream(sqlResult, Row.class).print("sql");

        // 5.执行环境
        env.execute("Flink_Table_Demo1");
    }
}
