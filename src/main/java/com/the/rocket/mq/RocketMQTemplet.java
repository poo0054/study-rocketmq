package com.the.rocket.mq;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 引入${@link RocketMQTemplet} 就可以发送rocketmq消息
 * TODO 待添加注解 aop实现消费者
 * TODO 待添加序列化配置
 * TODO 待添加mq发送消息事务 存入cache中
 *
 * @author zhangzhi
 * Date: 2022/4/13 9:46
 */
@Slf4j
@Data
public class RocketMQTemplet implements InitializingBean, DisposableBean {

    /**
     * 发送消息实体对象
     */
    private DefaultMQProducer producer;

    public DefaultMQProducer DefaultMQProducerTemplate() {
        return this.producer;
    }

    /**
     * TODO 待添加序列化
     */
    private Object serialize;

    /**
     * 顺序消息
     */
    private MessageQueueSelector messageQueueSelector = new SelectMessageQueueByHash();

    /**
     * TransactionMQProducer事务消息缓存
     */
    private final Map<String, TransactionMQProducer> cache = new ConcurrentHashMap<>();

    /**
     * 默认值
     */
    private String topic;


    @Override
    public void destroy() {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
        }
        cache.forEach((key, value) -> {
            value.shutdown();
        });
        cache.clear();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (producer != null) {
            producer.start();
        }
    }

    /**
     * 带事务的延时消息
     * 带参数
     *
     * @param body
     * @param sendCallback
     * @param level          level=0 级表示不延时，level=1 表示 1 级延时，level=2 表示 2 级延时，以此类推。
     *                       messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * @param userProperties
     * @param timeout
     */
    public void sendAsync(Object body, SendCallback sendCallback, Long timeout, int level, Map<String, String> userProperties) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        addUserproperties(msg, userProperties);
        msg.setDelayTimeLevel(level);
        doAsyncSend(msg, sendCallback, timeout);
    }


    /**
     * 异步延时消息
     *
     * @param body
     * @param sendCallback
     * @param level        level=0 级表示不延时，level=1 表示 1 级延时，level=2 表示 2 级延时，以此类推。
     *                     messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * @param timeout
     */
    public void sendAsync(Object body, SendCallback sendCallback, Long timeout, int level) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        msg.setDelayTimeLevel(level);
        doAsyncSend(msg, sendCallback, timeout);
    }

    /**
     * 同步消息
     *
     * @param body
     * @param timeout
     * @param level   level=0 级表示不延时，level=1 表示 1 级延时，level=2 表示 2 级延时，以此类推。
     *                messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * @param level
     */
    public SendResult send(Object body, Long timeout, int level) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        msg.setDelayTimeLevel(level);
        return doSend(msg, timeout);
    }

    /**
     * 异步消息 带延时
     *
     * @param body
     * @param sendCallback
     * @param messageQueueSelector
     * @param args
     * @param timeout
     * @param level                level=0 级表示不延时，level=1 表示 1 级延时，level=2 表示 2 级延时，以此类推。
     *                             messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * @return
     */
    public void sendAsync(Object body, SendCallback sendCallback, MessageQueueSelector messageQueueSelector, Object args, Long timeout, int level) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        msg.setDelayTimeLevel(level);
        doAsyncSend(msg, messageQueueSelector, args, sendCallback, timeout);
    }


    /**
     * 异步消息
     *
     * @param body
     * @return
     */
    public void sendAsync(Object body, SendCallback sendCallback, Long timeout) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        doAsyncSend(msg, sendCallback, timeout);
    }


    /**
     * 发送消息
     *
     * @param body
     * @return
     */
    public SendResult send(Object body) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        return doSend(msg, 3000L);
    }

    /**
     * 发送消息
     *
     * @param body
     * @return
     */
    public SendResult send(Object body, Map<String, String> userProperties) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        addUserproperties(msg, userProperties);
        return doSend(msg, 3000L);
    }

    /**
     * 发送消息
     *
     * @param body
     * @param level          0 级表示不延时，level=1 表示 1 级延时，level=2 表示 2 级延时，以此类推。
     *                       messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * @param userProperties
     * @return
     */
    public SendResult send(Object body, int level, Map<String, String> userProperties) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        addUserproperties(msg, userProperties);
        msg.setDelayTimeLevel(level);
        return doSend(msg, 3000L);
    }

    /**
     * 发送消息
     *
     * @param body
     * @param timeout
     * @return
     */
    public SendResult send(Object body, Long timeout) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, toDefaultSerialize(body));
        return doSend(msg, timeout);
    }


    /**
     * 发送消息
     *
     * @param tag
     * @param body
     * @param timeout
     * @return
     */
    public SendResult send(String tag, Object body, Long timeout) {
        if (StringUtils.isEmpty(topic)) {
            log.error("rocketmq.producer.topic参数未配置");
            throw new RuntimeException("rocketmq.producer.topic参数未配置");
        }
        Message msg = new Message(topic, tag, toDefaultSerialize(body));
        return doSend(msg, timeout);
    }


    /**
     * 发送消息
     *
     * @param topic
     * @param tag
     * @param body
     * @param timeout
     * @return
     */
    public SendResult send(String topic, String tag, Object body, Long timeout) {
        Message msg = new Message(topic, tag, toDefaultSerialize(body));
        return doSend(msg, timeout);
    }


    /**
     * 发送单向消息，没有任何返回结果
     *
     * @param topic
     * @param body
     */
    public void sendOneway(String topic, Object body) throws RemotingException, InterruptedException, MQClientException {
        Message msg = new Message(topic, toDefaultSerialize(body));
        // 发送单向消息，没有任何返回结果
        doSendOneway(msg);
    }

    /**
     * 发送单向消息，没有任何返回结果
     *
     * @param topic
     * @param tag
     * @param body
     */
    public void sendOneway(String topic, String tag, Object body) {
        Message msg = new Message(topic, tag, toDefaultSerialize(body));
        doSendOneway(msg);
    }


    /**
     * 发送单向消息，没有任何返回结果
     *
     * @param body
     */
    public void sendOneway(Object body) {
        Message msg = new Message(topic, toDefaultSerialize(body));
        // 发送单向消息，没有任何返回结果
        doSendOneway(msg);
    }

    /**
     * 发送单向消息，没有任何返回结果
     *
     * @param msg
     */
    public void doSendOneway(Message msg) {
        try {
            this.producer.sendOneway(msg);
        } catch (Exception e) {
            log.error("发送单向消息失败 topic:{} 内容为：{}  错误：{}", msg.getTopic(), JSON.toJSONString(new String(msg.getBody(), StandardCharsets.UTF_8)), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    /**
     * 发送消息
     *
     * @param msg
     * @param selector
     * @param arg
     * @param timeout
     * @return
     */
    public SendResult doSend(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        try {
            return this.producer.send(msg, selector, arg, timeout);
        } catch (Exception e) {
            log.error("发送消息  topic:{} 内容为：{}  错误：{}", msg.getTopic(), JSON.toJSONString(new String(msg.getBody(), StandardCharsets.UTF_8)), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    /**
     * 发送消息
     *
     * @param msg     消息
     * @param timeout 超时时间
     */
    public SendResult doSend(Message msg, Long timeout) {
        try {
            return this.producer.send(msg, timeout);
        } catch (Exception e) {
            log.error("发送消息  topic:{} 内容为：{}  错误：{}", msg.getTopic(), JSON.toJSONString(new String(msg.getBody(), StandardCharsets.UTF_8)), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

    }


    /**
     * 异步发送消息
     *
     * @param msg
     * @param selector
     * @param sendCallback
     */
    public void doAsyncSend(Message msg, MessageQueue selector, SendCallback sendCallback) {
        try {
            this.producer.send(msg, selector, sendCallback);
        } catch (Exception e) {
            log.error("发送消息  topic:{} 内容为：{}  错误：{}", msg.getTopic(), JSON.toJSONString(new String(msg.getBody(), StandardCharsets.UTF_8)), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    /**
     * 异步发送消息
     *
     * @param msg          消息
     * @param sendCallback 事务
     * @param timeout      超时时间
     */
    public void doAsyncSend(Message msg, SendCallback sendCallback, Long timeout) {
        try {
            this.producer.send(msg, sendCallback, timeout);
        } catch (Exception e) {
            log.error("发送消息  topic:{} 内容为：{}  错误：{}", msg.getTopic(), JSON.toJSONString(new String(msg.getBody(), StandardCharsets.UTF_8)), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    /**
     * 异步发送消息
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout
     */
    public void doAsyncSend(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) {
        try {
            this.producer.send(msg, selector, arg, sendCallback, timeout);
        } catch (Exception e) {
            log.error("发送消息  topic:{} 内容为：{}  错误：{}", msg.getTopic(), JSON.toJSONString(new String(msg.getBody(), StandardCharsets.UTF_8)), e.getMessage());
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 添加一些属性
     *
     * @param msg            消息对象
     * @param userProperties 属性对象
     */
    public void addUserproperties(Message msg, Map<String, String> userProperties) {
        for (String key : userProperties.keySet()) {
            String value = userProperties.get(key);
            msg.putUserProperty(key, value);
        }
    }

    /**
     * TODO 使用配置自定义序列化
     * 转换成byte[]
     *
     * @param o
     * @return
     */
    public byte[] toDefaultSerialize(Object o) {
        String s = JSON.toJSONString(o);
        return s.getBytes(StandardCharsets.UTF_8);
    }


}
