package com.codeh.watermark;

import com.codeh.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_WaterMark_Late
 * @date 2021/8/17 16:20
 * @description 使用watermark，allowedLateness，sideOutputLateData三重保证数据的准确性
 */
public class Flink_WaterMark_Late {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 设置事件时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1)) // 允许1分钟的迟到数据
                .sideOutputLateData(outputTag)  // 如果在1分钟之后在那个窗口还有迟到数据，将数据通过侧输出流输出
                .minBy("temperature");

        minStream.print("minTmp");
        minStream.getSideOutput(outputTag).print("late");

        environment.execute("Flink_WaterMark_Late");
    }
}
