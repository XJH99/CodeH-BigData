package com.codeh.source;

import com.codeh.bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FlinkSource4_MySource
 * @date 2021/8/11 14:06
 * @description 自定义数据源
 */
public class FlinkSource4_MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> source = env.addSource(new MySensor());

        // 打印输出
        source.print();

        env.execute("FlinkSource4_MySource");
    }

    /**
     * 自定义数据源
     */
    public static class MySensor implements SourceFunction<SensorReading> {

        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();

            HashMap<String, Double> map = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                // random.nextGaussian()一个正太分布
                map.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String sensorId : map.keySet()) {
                    // 获取一个新的温度值
                    double newTmp = map.get(sensorId) + random.nextGaussian();
                    map.put(sensorId, newTmp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTmp));
                }

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
