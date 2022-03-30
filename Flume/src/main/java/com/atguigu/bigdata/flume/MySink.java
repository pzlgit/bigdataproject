package com.atguigu.bigdata.flume;


import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 自定义Sink
 */
public class MySink extends AbstractSink implements Configurable {

    // 创建Logger对象，用于输出日志
    private static final Logger LOG = LoggerFactory.getLogger(MySink.class);

    /**
     * 读取配置文件中获取前缀名和后缀名
     */
    private String prefix;
    private String suffix;

    /**
     * 业务逻辑处理
     */
    public Status process() throws EventDeliveryException {
        // 声明返回值状态信息
        Status status;
        // 获取当前Sink绑定的Channel
        Channel channel = getChannel();
        // 获取事务
        Transaction transaction = channel.getTransaction();
        // 声明事件
        Event event;
        // 开始事务
        transaction.begin();
        // 读取Channel中的事件，直到读取到事件结束循环
        do {
            event = channel.take();
        } while (event == null);
        try {
            // 提交事务并打印数据信息
            LOG.info("--data:{}", prefix + new String(event.getBody()) + suffix);
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            // 回滚事务并输出报错日志
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
        } finally {
            // 关闭事务
            transaction.close();
        }
        return status;
    }

    /**
     * 全局配置信息
     */
    public void configure(Context context) {
        prefix = context.getString("prefix", "+");
        suffix = context.getString("suffix", "-");
    }

}