package com.the.rocket.mq;


import com.the.rocket.annotation.RocketMQListener;

/**
 * 继承继承该类 加入{@link RocketMQListener}注解实现监控
 *
 * @author zhangzhi
 * Date: 2022/4/13 16:26
 */
public interface MqListener<T> {


    void onMessage(T message);


}
