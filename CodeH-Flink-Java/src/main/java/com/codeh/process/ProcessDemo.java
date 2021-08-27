package com.codeh.process;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className ProcessDemo
 * @date 2021/8/24 11:10
 * @description 监控温度传感器的温度值，如果温度值在10秒钟之内连续上升，则进行报警处理
 */
public class ProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> source = env.socketTextStream("192.168.214.136", 9999);

        DataStream<SensorReading> mapStream = source.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

//        mapStream.keyBy("id") 这个返回值其实是一个Tuple类型
        mapStream.keyBy(row -> row.getId())
                .process(new MyProcessFunction(10))
                .print();

        env.execute("ProcessDemo");
    }

    /**
     * KeyedProcessFunction<K, I, O>
     * K:分组key的类型
     * I:输入的类型
     * O:输出的类型
     */
    public static class MyProcessFunction extends KeyedProcessFunction<String, SensorReading, String> {

        private Integer interval;

        public MyProcessFunction(Integer interval) {
            this.interval = interval;
        }

        private ValueState<Double> lastTempState;
        private ValueState<Long> timeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义两个状态，一个保存温度，一个保存处理时间
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double preTemp = lastTempState.value();
            Long preTime = timeState.value();

            // 上一个温度小于当前温度，并且定时器不存在
            if (preTemp != null) {  // 不做判断可能会出现空指针异常
                if (preTemp < value.getTemperature() && preTime == null) {
                    // 注册一个定时器
                    long processTime = ctx.timerService().currentProcessingTime() + interval * 1000;
                    ctx.timerService().registerProcessingTimeTimer(processTime);

                    // 更新定时器状态
                    timeState.update(processTime);
                } else {
                    // 移除定时器，并清空定时器的状态
                    ctx.timerService().deleteProcessingTimeTimer(preTime);
                    timeState.clear();
                }
            }
            // 更新温度值
            lastTempState.update(value.getTemperature());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发操作
            out.collect("传感器" + ctx.getCurrentKey() + "的温度值连续" + interval + "秒上升");

            // 触发定时操作过后清空定时器状态
            timeState.clear();
        }
    }
}
