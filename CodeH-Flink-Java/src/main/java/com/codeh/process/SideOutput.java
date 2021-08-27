package com.codeh.process;

import com.codeh.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className SideOutput
 * @date 2021/8/27 10:27
 * @description 使用侧输出流来对温度划分为高温流与低温流，主流输出高温流，侧输出流输出低温流
 */
public class SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = environment.socketTextStream("192.168.214.136", 9999);

        SingleOutputStreamOperator<SensorReading> mapSource = source.map(data -> {
            String[] split = data.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        final OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("tag") {
        };

        SingleOutputStreamOperator<SensorReading> process = mapSource.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    // 侧输出流输出
                    ctx.output(outputTag, value);
                }
            }
        });

        process.print("主流输出");
        // 获取侧输出流，并进行输出
        process.getSideOutput(outputTag).print("侧输出流输出");

        environment.execute("SideOutput");
    }
}
