package com.the.rocket.test;

import com.the.rocket.mq.RocketMQTemplet;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

/**
 * mq测试发送消息
 *
 * @author zhangzhi
 * Date: 2022/4/14 15:54
 */
@Slf4j
@RestController
@RequestMapping("/api/sendTest")
public class SendTest {

    @Autowired
    RocketMQTemplet rocketMQtemplet;

    /**
     * 发送消息
     *
     * @param msg   需要发送的内容
     * @param i     putUserProperty设置属性进行过滤
     * @param level 延时级别
     *              0 级表示不延时，level=1 表示 1 级延时，level=2 表示 2 级延时，以此类推。
     *              messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * @return
     */
    @PostMapping("/send/{msg}/{i}/{level}")
    public SendResult send(@PathVariable("msg") String msg, @PathVariable("i") String i, @PathVariable("level") Integer level) {
        HashMap<String, String> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put("a", i);
        SendResult send = rocketMQtemplet.send(msg, level, objectObjectHashMap);
        System.out.println("发送成功！ " + send);
        return send;
    }
}
