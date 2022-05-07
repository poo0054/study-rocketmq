package com.the.rocket.annotation;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.annotation.*;


/**
 * @author zhangzhi
 * @version 1.0
 * @date 2022/5/7 9:56
 */

@Target(ElementType.TYPE)
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketMQListener {

    /**
     * 消费者分组
     *
     * @return
     */
    String consumerGroup() default "DEFAULT_CONSUMER_GROUP";

    /**
     * NameServer的地址 不填使用配置文件的
     *
     * @return
     */
    String nameserverAdder() default "";

    /**
     * topic
     *
     * @return
     */
    String topic() default "DEFAULT_TOPIC";

    /**
     * 消息跟踪的切换标志实例
     *
     * @return
     */
    boolean enableMsgTrace() default false;

    /**
     * 參考 {@link AccessChannel}
     *
     * @return
     */
    AccessChannel accessChannel() default AccessChannel.LOCAL;

    /**
     * 最大线程
     *
     * @return
     */
    int consumeThreadMax() default 20;

    /**
     * 超时时间
     *
     * @return
     */
    int consumeTimeout() default 15;

    /**
     * 是否为sql模式
     *
     * @return
     */
    boolean tagType() default false;

    /**
     * tag的值
     *
     * @return
     */
    String tagValue() default "*";


    /**
     * 消息模式 参考：{@link MessageModel}
     *
     * @return
     */
    MessageModel messageModel() default MessageModel.CLUSTERING;

    /**
     * 是否使用按照循序查询
     *
     * @return
     */
    boolean consumeMode() default false;

    /**
     * 顺序查询的类型 参考{@link ConsumeFromWhere}
     *
     * @return
     */
    ConsumeFromWhere consume_from_where() default ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

}
