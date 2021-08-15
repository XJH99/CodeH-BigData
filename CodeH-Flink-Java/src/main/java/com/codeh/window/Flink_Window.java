package com.codeh.window;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className Flink_Window
 * @date 2021/8/13 18:06
 * @description Flink Window相关操作
 */
public class Flink_Window {
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

        // 增量聚合函数测试
//        DataStream<Integer> aggregateStream = mapStream.keyBy("id")
//                .timeWindow(Time.seconds(15))
//                .aggregate(new CountAgg());

        DataStream<Tuple3<String, Long, Integer>> applyStream = mapStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new MyWindowFunction());

        applyStream.print();

        environment.execute("Flink_Window");

    }

    /**
     * 增量聚合函数
     * AggregateFunction<IN, ACC, OUT>
     * IN：输入数据类型
     * ACC：累加器，数据累加类型
     * OUT：输出数据类型
     */
    public static class CountAgg implements AggregateFunction<SensorReading, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(SensorReading value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    /**
     * 全窗口函数
     * WindowFunction<IN, OUT, KEY, W extends Window>
     * IN:输入数据类型
     * OUT：输出数据类型
     * KEY：分组的key
     * Window：时间窗口类型
     */
    public static class MyWindowFunction implements WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
            // 1.获取id
            String id = tuple.getField(0);

            // 2.获取统计的数量
            int count = 0;
            Iterator<SensorReading> iterator = input.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count = count + 1;
            }

            // 3.获取时间
            long end = window.getEnd();

            // 4.输出数据
            out.collect(new Tuple3<>(id, end, count));

        }
    }
}
