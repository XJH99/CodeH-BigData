package com.codeh.source;

import com.codeh.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FlinkSource1_Collection
 * @date 2021/8/11 14:06
 * @description 从文件中读取数据
 */
public class FlinkSource2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.readTextFile("CodeH-Flink-Java/src/main/resources/word.txt");

        // 打印输出
        streamSource.print();

        env.execute("FlinkSource2_File");
    }
}
