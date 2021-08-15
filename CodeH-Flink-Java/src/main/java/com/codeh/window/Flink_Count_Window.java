package com.codeh.window;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Count_Window
 * @date 2021/8/15 21:04
 * @description 计数窗口测试
 */
public class Flink_Count_Window {
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

        // 计算在滑动计数窗口内，相同key的平均温度
        DataStream<Double> aggregateStream = mapStream.keyBy("id")
                .countWindow(10, 2) // 窗口大小为10，滑动窗口大小为2
                .aggregate(new MyAvgTemp());

        aggregateStream.print();

        environment.execute("Flink_Count_Window");

    }

    /**
     * AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>
     * Tuple2<Double, Integer>: 一个是输入温度的总和，一个是输入的个数
     * Double：输出的平均温度值
     */
    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            // 温度叠加，个数加一
            return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
