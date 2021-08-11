package com.codeh.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className WordCount
 * @date 2021/8/11 10:30
 * @description wordCount批处理模式
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //1.获取上下文环境变量
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> source = env.readTextFile("D:\\IDEA_Project\\CodeH-BigData\\CodeH-Flink-Java\\src\\main\\resources\\word.txt");

        DataSet<Tuple2<String, Integer>> res = source.flatMap(new MyFlatMap())
                .groupBy(0)
                .sum(1);

        res.print();

    }

    /**
     * FlatMapFunction<T, O>
     * T:输入的参数类型
     * O:输出的参数类型
     */
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = input.split(" ");

            for (String word : words) {
                // 输出我们想要的结果类型
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}


