package com.codeh.transform;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Transform
 * @date 2021/8/11 15:20
 * @description Flink转换算子的使用方式
 */
public class Flink_Transform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("CodeH-Flink-Java/src/main/resources/Sensor.txt");

        DataStream<SensorReading> mapStream = source.map(new MyMapFunction());

//        DataStream<String> flatMapStream = source.flatMap(new MyFlatMapFunction());

//        DataStream<String> filterStream = source.filter(new MyFilterFunction());

        /**
         * 使用keyBy分组时，实体类必须得加上无参构造器
         * minBy：输出的是最小值的那一行数据,
         * min：输出的还是第一条读取的数据，只是将比较的字段取了最小值
         */
//        DataStream<SensorReading> minStream = mapStream.keyBy("id")
//                .minBy("temperature");

//        DataStream<SensorReading> reduceStream = mapStream.keyBy("id")
//                .reduce(new MyReduceFunction());

        SplitStream<SensorReading> splitStream = mapStream.split(new MySplitFunction());

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");

        highStream.print("high stream");
        lowStream.print("low stream");

        env.execute("Flink_Transform");


    }

    /**
     * MapFunction<T, O>
     * T：输入参数类型
     * O：输出参数类型
     */
    public static class MyMapFunction implements MapFunction<String, SensorReading> {

        @Override
        public SensorReading map(String input) throws Exception {
            String[] split = input.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }
    }

    /**
     * FlatMapFunction<T, O>
     * T: 输入参数类型
     * O: 输出参数类型
     */
    public static class MyFlatMapFunction implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String input, Collector<String> out) throws Exception {
            String[] splits = input.split(",");

            for (String split : splits) {
                out.collect(split);
            }
        }
    }

    /**
     * FilterFunction<T>
     * T: 输入参数类型
     */
    public static class MyFilterFunction implements FilterFunction<String> {

        @Override
        public boolean filter(String value) throws Exception {
            // 将小于37.3的数据返回
            String[] split = value.split(",");
            return Double.parseDouble(split[2]) < 37.3;
        }
    }

    /**
     * ReduceFunction<T>
     * T: 该函数处理的数据类型
     */
    public static class MyReduceFunction implements ReduceFunction<SensorReading> {

        @Override
        public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
            // reduce聚合，取出最小的温度值，并输出当前的时间戳
            return new SensorReading(value1.getId(), value2.getTimestamp(), Math.min(value1.temperature, value2.temperature));
        }
    }

    /**
     * OutputSelector<OUT>
     * 分流操作：根据条件将流分成不同的支流
     */
    public static class MySplitFunction implements OutputSelector<SensorReading> {

        @Override
        public Iterable<String> select(SensorReading value) {
            return (value.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
        }
    }
}
