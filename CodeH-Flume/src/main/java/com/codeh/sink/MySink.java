package com.codeh.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义 sink 功能
 * 需求： 使用 flume 接收数据，并在 Sink 端给每条数据添加前缀和后缀，输出到控制台
 */
public class MySink extends AbstractSink implements Configurable {

    private String prefix;
    private String suffix;

    // 声明日志对象
    private static final Logger LOG = LoggerFactory.getLogger(MySink.class);

    // 1.初始化配置文件
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello");
        //读取配置文件内容，无默认值
        suffix = context.getString("suffix");
    }

    // 2.实际业务操作
    public Status process() throws EventDeliveryException {
        // 3.初始化状态值
        Status status = null;

        // 4.获取channel对象
        Channel channel = getChannel();

        // 5.获取channel的事务对象
        Transaction transaction = channel.getTransaction();

        // 6.开启事务
        transaction.begin();

        try {
            // 7.从channel中获取事件数据
            Event take = channel.take();

            if (take != null) {
                String body = new String(take.getBody());
                // 8.将事件数据用日志打印出来
                LOG.info(prefix + "--" + body + "--" + suffix);
            }

            // 9.提交事务
            transaction.commit();

            // 10.成功提交，修改状态信息
            status = Status.READY;
        } catch (ChannelException e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        } finally {
            // 事务关闭
            transaction.close();
        }

        return status;
    }


}
