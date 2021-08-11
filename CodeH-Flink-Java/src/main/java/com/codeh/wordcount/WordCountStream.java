package com.codeh.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author jinhua.xu
 * @version 1.0
 * @className WordCountStream
 * @date 2021/8/11 10:56
 * @description WordCount流处理模式
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.214.136", 9999);

        DataStream<Tuple2<String, Integer>> res = source.flatMap(new WordCountBatch.MyFlatMap())
                .keyBy(0)
                .sum(1);

        res.print().setParallelism(1);

        env.execute();
    }
}
