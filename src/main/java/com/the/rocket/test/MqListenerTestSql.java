package com.the.rocket.test;

import com.the.rocket.annotation.RocketMQListener;
import com.the.rocket.mq.MqListener;
import org.springframework.stereotype.Component;

/**
 * 监听用法用法 匹配过滤消息
 * 详情参考
 * <a href="https://github.com/apache/rocketmq/blob/master/docs/cn/RocketMQ_Example.md#21-%E9%A1%BA%E5%BA%8F%E6%B6%88%E6%81%AF%E7%94%9F%E4%BA%A7">顺序消费</a>
 *
 * @author zhangzhi
 * Date: 2022/4/13 17:52
 */
@Component
@RocketMQListener(tagType = true, tagValue = "a between 0 and 3")
public class MqListenerTestSql implements MqListener<String> {


    @Override
    public void onMessage(String message) {
        System.out.println("MqListenerTestSql 接收到的消息为：" + message);
    }
}
