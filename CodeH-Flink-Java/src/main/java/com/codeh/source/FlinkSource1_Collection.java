package com.codeh.source;

import com.codeh.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FlinkSource1_Collection
 * @date 2021/8/11 14:06
 * @description 从集合中读取数据
 */
public class FlinkSource1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> streamSource = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1628662456L, 35.8),
                        new SensorReading("sensor_2", 1628662492L, 15.4),
                        new SensorReading("sensor_3", 1628662500L, 5.7),
                        new SensorReading("sensor_4", 1628662507L, 38.8)
                )
        );

        // 打印输出
        streamSource.print();

        env.execute("FlinkSource1_Collection");
    }
}
