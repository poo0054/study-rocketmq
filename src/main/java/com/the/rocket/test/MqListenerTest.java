package com.the.rocket.test;


import com.the.rocket.annotation.RocketMQListener;
import com.the.rocket.mq.MqListener;
import org.springframework.stereotype.Component;

/**
 * 监听用法用法
 *
 * @author zhangzhi
 * Date: 2022/4/13 17:52
 */
@Component
@RocketMQListener(consumerGroup = "api_consumer_sql")
public class MqListenerTest implements MqListener<String> {


    @Override
    public void onMessage(String message) {
        System.out.println(" MqListenerTest 收到的消息为：" + message);
    }
}
