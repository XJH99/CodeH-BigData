package com.codeh.web.frauddetection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FraudDetectionJob
 * @date 2021/8/23 16:08
 * @description 官网欺诈检测案例
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts.addSink(new AlertSink())
                .name("send-alerts");

        env.execute("Fraud Detection");
    }

    /**
     * Skeleton code for implementing a fraud detector.
     */
    public static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

        private static final long serialVersionUID = 1L;

        private static final double SMALL_AMOUNT = 1.00;
        private static final double LARGE_AMOUNT = 500.00;
        private static final long ONE_MINUTE = 60 * 1000;

        // 定义一个判断状态
        private ValueState<Boolean> flagState;
        // 定义一个定时器状态
        private ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            flagState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("flag", Boolean.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }

        @Override
        public void processElement(
                Transaction transaction,
                Context context,
                Collector<Alert> collector) throws Exception {

            // 获取状态值
            Boolean flag = flagState.value();

            if (flag != null) {
                // 当标识不为空时
                if (transaction.getAmount() > LARGE_AMOUNT) {
                    // 如果账号消费金额大于500.00，输出为异常账号
                    Alert alert = new Alert();
                    alert.setId(transaction.getAccountId());
                    collector.collect(alert);
                }
                // 清除所有的状态
                cleanUp(context);
            }

            if (transaction.getAmount() < SMALL_AMOUNT) {
                // 当金额小于1.00设置状态为true
                flagState.update(true);

                // 获取当前本地处理时间
                long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;

                // 注册定时器
                context.timerService().registerProcessingTimeTimer(timer);

                // 更新时间状态
                timerState.update(timer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
            // remove flag after 1 minute 定时清除状态
            timerState.clear();
            flagState.clear();
        }

        private void cleanUp(Context ctx) throws Exception {
            // 删除定时器
            Long timer = timerState.value();
            ctx.timerService().deleteProcessingTimeTimer(timer);

            // 清除所有的状态
            timerState.clear();
            flagState.clear();
        }
    }
}
