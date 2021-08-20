package com.codeh.state;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Operator_State
 * @date 2021/8/19 16:36
 * @description Flink中简单状态编程，检测温度变化进行报警处理
 */
public class Flink_Operator_State {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 设置事件时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        DataStreamSource<String> source = environment.socketTextStream("192.168.214.136", 9999);

        DataStream<SensorReading> mapStream = source.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        SingleOutputStreamOperator<Tuple3<String, Double, Double>> tempStream = mapStream.keyBy("id")
                .flatMap(new TempIncreaseWarning(10.0));

        tempStream.print();

        environment.execute("Flink_Operator_State");
    }


    /**
     * 温度报警监控实现类
     */
    public static class TempIncreaseWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        // 私有属性，温度跳变差值
        private Double threshold;

        // 定义状态，保存上一次的温度值
        private ValueState<Double> lastTemperature;

        public TempIncreaseWarning(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<>("value_state", Double.class));
        }


        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 1.获取当前状态里面的温度值
            Double t1 = lastTemperature.value();

            // 2.当保存的温度状态值不为null值时
            if (t1 != null) {
                double diff = Math.abs(value.getTemperature() - t1);

                // 3.如果温度相差10度，则报警
                if (diff > threshold) {
                    out.collect(new Tuple3<>(value.getId(), t1, value.getTemperature()));
                }
            }

            // 4.更新温度状态值
            lastTemperature.update(value.getTemperature());
        }

        // 清理状态
        @Override
        public void close() throws Exception {
            lastTemperature.clear();
        }
    }

//    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
//
//        // 定义一个本地变量
//        private Integer count = 0;
//
//        @Override
//        public Integer map(SensorReading value) throws Exception {
//            count ++;
//            return count;
//        }
//
//        @Override
//        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
//            return Collections.singletonList(count);
//        }
//
//        @Override
//        public void restoreState(List<Integer> state) throws Exception {
//            for (Integer num: state) {
//                count += num;
//            }
//        }
//    }
}
