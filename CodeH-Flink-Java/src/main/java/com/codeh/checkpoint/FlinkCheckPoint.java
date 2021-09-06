package com.codeh.checkpoint;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

import java.io.IOException;

/**
 * @author jinhua.xu
 * @version 1.0
 * @className FlinkCheckPoint
 * @date 2021/8/28 21:51
 * @description 状态后端以及检查点checkpoint的设置
 */
public class FlinkCheckPoint {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 内存级别的状态后端
        environment.setStateBackend(new MemoryStateBackend());
        /**
         * 第一个参数表示文件系统路径
         * 第二个参数表示使用同步还是异步的方式将状态数据同步到文件系统中，默认是异步
         */
        environment.setStateBackend(new FsStateBackend("hdfs://", true));

        /**
         * 参数为文件系统路径
         */
        environment.setStateBackend(new RocksDBStateBackend("hdfs://"));

        /**
         * 启用检查点，参数表示检查点生成的间隔
         */
        environment.enableCheckpointing(1000);

        // 设置检查为精准一次性
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点超时时间
        environment.getCheckpointConfig().setCheckpointTimeout(500);
        // 设置检查点最大并发数
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 触发下一个检查点的时间间隔
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 当同时存在检查点以及保存点时，这个配置更加愿意使用检查点进行容灾处理
        environment.getCheckpointConfig().setPreferCheckpointForRecovery(true);

    }
}
