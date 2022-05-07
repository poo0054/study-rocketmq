package com.the.rocket.mq;

import com.alibaba.fastjson.JSON;
import com.the.rocket.annotation.RocketMQListener;
import com.the.rocket.config.RocketProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

/**
 * 监听器真正执行的方法
 *
 * @author zhangzhi
 * Date: 2022/4/13 15:24
 */
@Slf4j
@Data
public class RocketMQConsumerListenerContainer implements InitializingBean, SmartLifecycle, ApplicationContextAware {
    private RocketMQListener rocketMQListener;

    private long suspendCurrentQueueTimeMillis = 1000;

    private AccessChannel accessChannel = AccessChannel.LOCAL;

    private String charset = "UTF-8";
    /**
     * 是否在运行
     */
    private boolean running;

    /**
     * 服务器名称
     */
    private String nameSrvAddr;

    /**
     * 实例名称
     */
    private String name;

    /**
     * 执行后对象
     */
    private MqListener listener;

    /**
     * 泛型
     */
    private Class messageType;

    /**
     * mq全局配置
     */
    private RocketProperties rocketProperties;

    public RocketMQConsumerListenerContainer(RocketProperties rocketProperties) {
        this.rocketProperties = rocketProperties;
    }

    /**
     * mq消费者对象
     */
    private DefaultMQPushConsumer consumer;

    private ApplicationContext applicationContext;

    @Override
    public void afterPropertiesSet() throws Exception {
        initRocketMQPushConsumer();
        log.debug("RocketMQ 监听器  启动  名称:{}", getName());
        /**
         * 获取泛型对象
         */
        this.messageType = getMessageType();
    }

    /**
     * 获取泛型对象
     *
     * @return
     */
    @SuppressWarnings("WeakerAccess")
    private Class getMessageType() {
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(listener);
        Type[] interfaces = targetClass.getGenericInterfaces();
        Class<?> superclass = targetClass.getSuperclass();
        while ((Objects.isNull(interfaces) || 0 == interfaces.length) && Objects.nonNull(superclass)) {
            interfaces = superclass.getGenericInterfaces();
            superclass = targetClass.getSuperclass();
        }
        if (Objects.nonNull(interfaces)) {
            for (Type type : interfaces) {
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    if (Objects.equals(parameterizedType.getRawType(), MqListener.class)) {
                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                        if (!ObjectUtils.isEmpty(actualTypeArguments)) {
                            Class actualTypeArgument = (Class) actualTypeArguments[0];
                            return actualTypeArgument;
                        }
                    }
                }
            }
        }
        return Object.class;
    }

    /**
     * 初始化
     * 就近原则
     */
    private void initRocketMQPushConsumer() throws MQClientException {
        Assert.notNull(rocketMQListener, "rocketMQListener不能为空");

        String nameSrvAddr = rocketMQListener.nameserverAdder();
        if (StringUtils.isEmpty(nameSrvAddr)) {
            nameSrvAddr = rocketProperties.getNameSrvAddr();
        }

        String topic = rocketMQListener.topic();
        if (StringUtils.isEmpty(topic)) {
            topic = rocketProperties.getTopic();
        }
        Assert.notNull(topic, "topic不能为空");


        //使用就近原则
        String group = rocketMQListener.consumerGroup();
        if (StringUtils.isEmpty(group)) {
            group = rocketProperties.getConsumer().getConsumerGroup();
        }
        Assert.notNull(group, "consumerGroup 不能为空");

        /**
         * 认证服务没有导入jar包
         */
       /* RPCHook rpcHook = RocketMQUtil.getRPCHookByAkSk(applicationContext.getEnvironment(),
                this.rocketMQMessageListener.accessKey(), this.rocketMQMessageListener.secretKey());*/
//        boolean enableMsgTrace = rocketMQListener.enableMsgTrace();


       /* if (Objects.nonNull(rpcHook)) {
            consumer = new DefaultMQPushConsumer(consumerGroup, rpcHook, new AllocateMessageQueueAveragely(),
                    enableMsgTrace, this.applicationContext.getEnvironment().
                    resolveRequiredPlaceholders(this.rocketMQMessageListener.customizedTraceTopic()));
            consumer.setVipChannelEnabled(false);
            consumer.setInstanceName(RocketMQUtil.getInstanceName(rpcHook, consumerGroup));
        } else {
            log.debug("没有认证服务" + this + ".");
            consumer = new DefaultMQPushConsumer(consumerGroup, enableMsgTrace,
                    this.applicationContext.getEnvironment().
                            resolveRequiredPlaceholders(this.rocketMQMessageListener.customizedTraceTopic()));
        }*/


        boolean b = rocketMQListener.enableMsgTrace();

        consumer = new DefaultMQPushConsumer(group, b);


        consumer.setNamesrvAddr(nameSrvAddr);

        AccessChannel accessChannel = rocketMQListener.accessChannel();
        if (accessChannel != null) {
            consumer.setAccessChannel(accessChannel);
        }

        consumer.setConsumeThreadMax(rocketMQListener.consumeThreadMax());
        consumer.setConsumeTimeout(rocketMQListener.consumeTimeout());

        /**
         * 是否为sql模式
         */
        if (rocketMQListener.tagType()) {
            consumer.subscribe(topic, MessageSelector.bySql(rocketMQListener.tagValue()));
        } else {
            consumer.subscribe(topic, rocketMQListener.tagValue());
        }

        /**
         * 消息模式
         */
        switch (rocketMQListener.messageModel()) {
            case BROADCASTING:
                consumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.BROADCASTING);
                break;
            case CLUSTERING:
                consumer.setMessageModel(org.apache.rocketmq.common.protocol.heartbeat.MessageModel.CLUSTERING);
                break;
            default:
                throw new IllegalArgumentException("没有找到改模式");
        }

        /**
         * 是否使用按照循序查询
         */
        if (rocketMQListener.consumeMode()) {
            consumer.setConsumeFromWhere(rocketMQListener.consume_from_where());
            consumer.setMessageListener(new DefaultMessageListenerOrderly());
        } else {
            consumer.setMessageListener(new DefaultMessageListenerConcurrently());
        }

    }


    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("rocketmq监控消息: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    listener.onMessage(doConvertMessage(messageExt));
                    long costTime = System.currentTimeMillis() - now;
                    log.info("rocketmq消费消息msgId: {} 时间{}", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("rocketmq消息消费失败:{}  失败原因:{}", messageExt, e.getMessage());
                    context.setDelayLevelWhenNextConsume(0);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }


    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("rocketmq监控消息: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    listener.onMessage(doConvertMessage(messageExt));
                    long costTime = System.currentTimeMillis() - now;
                    log.info("rocketmq消费消息msgId: {} 时间{}", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("rocketmq消息消费失败:{}  失败原因:{}", messageExt, e.getMessage());
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    /**
     * 序列化使用json
     *
     * @param messageExt
     * @return
     */
    private Object doConvertMessage(MessageExt messageExt) {
        if (Objects.equals(messageType, MessageExt.class)) {
            return messageExt;
        } else {
            String str = new String(messageExt.getBody(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else {
                // json序列化  反序列化回来
                try {
                    return JSON.parseObject(str, messageType.getClass());
                } catch (Exception e) {
                    log.info("rocketmq转换失败 源类型:{}, 转换后类型:{}", str, messageType);
                    throw new RuntimeException("rocketmq转换失败 目标类型 " + messageType, e);
                }
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void start() {
        if (this.isRunning()) {
            throw new IllegalStateException("当前容器已经在运行 " + this.toString());
        }

        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new IllegalStateException("RocketMQ启动消费者失败 ", e);
        }
        this.setRunning(true);

        log.info("运行容器: {}", this.toString());
    }

    @Override
    public void stop() {
        if (this.isRunning()) {
            if (Objects.nonNull(consumer)) {
                consumer.shutdown();
            }
            setRunning(false);
        }
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }


}
