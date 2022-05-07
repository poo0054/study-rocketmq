package com.the.rocket.config;

import lombok.Data;
import org.apache.rocketmq.common.MixAll;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author zhangzhi
 * Date: 2022/4/12 18:19
 */
@ConfigurationProperties(prefix = "rocketmq")
@Data
public class RocketProperties {

    /**
     * 域名模式访问方式不支持分隔符(;)，只能设置一个域名。
     */
    private String nameSrvAddr;

    private String accessChannel;

    private Producer producer;
    /**
     * 生产者组在概念上聚合了完全相同角色的所有生产者实例，这在涉及事务性消息时尤其重要。
     * 对于非事务性消息，只要它在每个进程中都是唯一的，就没有关系。
     */
    private String producerGroup;

    private String topic;

    @Data
    public static class Producer {

        /**
         * 生产者的组名。
         */
        private String group;

        /**
         * 发送消息超时的毫秒数。
         */
        private int sendMessageTimeout = 3000;

        /**
         * 压缩消息体阈值，即默认压缩大于4k的消息体。
         */
        private int compressMessageBodyThreshold = 1024 * 4;

        /**
         * 在同步模式下声明发送失败之前内部执行的最大重试次数。这可能会导致消息重复，这取决于应用程序开发人员来解决。
         */
        private int retryTimesWhenSendFailed = 2;

        /**
         * 在异步模式下声明发送失败之前内部执行的最大重试次数。
         * 这可能会导致消息重复，这取决于应用程序开发人员来解决
         */
        private int retryTimesWhenSendAsyncFailed = 2;

        /**
         * 指示是否在内部发送失败时重试另一个代理
         */
        private boolean retryNextServer = false;

        /**
         * 允许的最大消息大小（以字节为单位）
         */
        private int maxMessageSize = 1024 * 1024 * 4;

        /**
         * access-key
         */
        private String accessKey;

        /**
         * secret-key
         */
        private String secretKey;

        /**
         * 消息跟踪的切换标志实例。
         */
        private boolean enableMsgTrace = true;

        /**
         * 消息跟踪主题的名称值。如果不配置，可以使用默认的跟踪主题名称
         */
        private String customizedTraceTopic = MixAll.RMQ_SYS_TRACE_TOPIC;


        //-------------------------------------------------


    }

    private Consumer consumer = new Consumer();

    @Data
    public static final class Consumer {

        private String consumerGroup;


    }
}
