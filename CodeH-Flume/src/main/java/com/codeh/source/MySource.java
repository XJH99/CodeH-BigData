package com.codeh.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/**
 * 自定义 source 功能
 * 需求：使用 flume 接收数据，并给每条数据添加前缀，输出到控制台
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    // 定义前缀与后缀的变量
    private String prefix;
    private String suffix;

    // 初始化配置信息
    public void configure(Context context) {
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "hypers"); // 没有配置就使用默认值
    }

    // 主要实现功能
    public Status process() throws EventDeliveryException {
        // 1.定义状态变量
        Status status = null;

        try {

            // 2.循环模拟数据的产生
            for (int i = 0; i < 5; i++) {
                // 3.创建事件对象
                SimpleEvent event = new SimpleEvent();

                event.setBody((prefix + "--" + i + "--" + suffix).getBytes());
                // 4.将事件写入到channel
                getChannelProcessor().processEvent(event);
            }
            // 5.将状态进行修改
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }

        // 休眠两秒
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();

        }

        // 返回状态
        return status;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
