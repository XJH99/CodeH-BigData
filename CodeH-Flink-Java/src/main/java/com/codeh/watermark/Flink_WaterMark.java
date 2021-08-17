package com.codeh.watermark;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_WaterMark
 * @date 2021/8/16 15:45
 * @description Flink WaterMark 处理延时数据的功能
 */
public class Flink_WaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 设置事件时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 100毫秒生成一次watermark
//        environment.getConfig().setAutoWatermarkInterval(100);

        // 读取数据
        DataStreamSource<String> source = environment.socketTextStream("192.168.214.136", 9999);

        // 封装成SensorReading实体类
        DataStream<SensorReading> mapStream = source.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(3)) { // 参数为最大乱序程度
            @Override
            public long extractTimestamp(SensorReading element) {
                // 返回的时间需要是一个毫秒值
                return element.getTimestamp() * 1000;
            }
        });

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .minBy("temperature");

//        DataStream<SensorReading> reduceStream = mapStream.keyBy("id")
//                .timeWindow(Time.seconds(10))
//                .reduce(new ReduceFunction<SensorReading>() {
//                    @Override
//                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                        return new SensorReading(value1.getId(), value2.getTimestamp(), Math.min(value1.getTemperature(), value2.getTemperature()));
//                    }
//                });

        minStream.print();

        environment.execute("Flink_WaterMark");
    }


}
