package com.codeh.transform;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Transform_Connect
 * @date 2021/8/11 18:56
 * @description
 */
public class Flink_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("CodeH-Flink-Java/src/main/resources/Sensor.txt");

        DataStream<SensorReading> mapStream = source.map(new Flink_Transform.MyMapFunction());

        SplitStream<SensorReading> splitStream = mapStream.split(new Flink_Transform.MySplitFunction());

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");

        /**
         * 下面为合流操作
         */
        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowStream);

        SingleOutputStreamOperator<Object> res = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "healthy");
            }
        });

        res.print();

        env.execute("Flink_Transform_Connect");

    }
}
