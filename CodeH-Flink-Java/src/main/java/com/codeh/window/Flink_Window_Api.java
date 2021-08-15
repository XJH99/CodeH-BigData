package com.codeh.window;

import com.codeh.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Window_Api
 * @date 2021/8/15 21:31
 * @description Flink Window中的一些其它可选API
 */
public class Flink_Window_Api {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 读取数据
        DataStreamSource<String> source = environment.socketTextStream("192.168.214.136", 9999);

        // 封装成SensorReading实体类
        DataStream<SensorReading> mapStream = source.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        OutputTag<SensorReading> lateStream = new OutputTag<SensorReading>("late") {
        };

        /**
         * 比如就是到9点关闭的窗口，9点准时会输出当时的结果，但是窗口不会关闭，到9：01时会将迟到的数据累加起来通过侧输出流输出结果
         */
        SingleOutputStreamOperator<SensorReading> sumStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(10))
//                .trigger() 触发器：定义window什么时候关闭，触发计算并输出结果
//                .evictor() 移除器：定义移除某些数据的逻辑
                .allowedLateness(Time.minutes(1))   // 允许迟到一分钟的数据
                .sideOutputLateData(lateStream)   // 将迟到的数据通过侧输出流输出
                .sum("temperature");

        sumStream.getSideOutput(lateStream).print();

        sumStream.print();

        environment.execute("Flink_Window_Api");
    }
}
