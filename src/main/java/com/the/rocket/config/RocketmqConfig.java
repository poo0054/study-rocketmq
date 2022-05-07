package com.the.rocket.config;

import com.the.rocket.mq.RocketMQConsumerHandler;
import com.the.rocket.mq.RocketMQTemplet;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.RPCHook;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.StringUtils;

/**
 * @author zhangzhi
 * @version 1.0
 * @date 2022/5/7 9:52
 */

@Slf4j
@Configuration
@Import({RocketMQConsumerHandler.class})
@ConditionalOnClass({DefaultMQProducer.class})
@ConditionalOnProperty(prefix = "rocketmq", value = "name-srv-addr", matchIfMissing = true)
@EnableConfigurationProperties(RocketProperties.class)
public class RocketmqConfig {

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    @ConditionalOnProperty(prefix = "rocketmq", value = {"name-srv-addr", "producer.group"})
    public DefaultMQProducer defaultMQProducer(RocketProperties rocketProperties) {
        RocketProperties.Producer rocketPropertiesProducer = rocketProperties.getProducer();
        DefaultMQProducer producer;
        String ak = rocketProperties.getProducer().getAccessKey();
        String sk = rocketProperties.getProducer().getSecretKey();

        if (!StringUtils.isEmpty(ak) && !StringUtils.isEmpty(sk)) {
            RPCHook rpcHook = null;
            //TODO  acl包导入，使用mq的认证功能
//            RPCHook rpcHook = new AclClientRPCHook(new SessionCredentials(ACL_ACCESS_KEY, ACL_SECRET_KEY));
            producer = new DefaultMQProducer(rocketPropertiesProducer.getGroup(), rpcHook,
                    rocketProperties.getProducer().isEnableMsgTrace(),
                    rocketProperties.getProducer().getCustomizedTraceTopic());
            producer.setVipChannelEnabled(false);
            new RuntimeException("acl包未导入,rocketmq认证无法使用 。请导入包后把注释打开重试");
        } else {
            producer = new DefaultMQProducer(rocketPropertiesProducer.getGroup(), rocketProperties.getProducer().isEnableMsgTrace(),
                    rocketProperties.getProducer().getCustomizedTraceTopic());
        }
        producer.setNamesrvAddr(rocketProperties.getNameSrvAddr());

        String accessChannel = rocketProperties.getAccessChannel();
        if (!StringUtils.isEmpty(accessChannel)) {
            producer.setAccessChannel(AccessChannel.valueOf(accessChannel));
        }
        producer.setSendMsgTimeout(rocketPropertiesProducer.getSendMessageTimeout());
        producer.setRetryTimesWhenSendFailed(rocketPropertiesProducer.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(rocketPropertiesProducer.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(rocketPropertiesProducer.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(rocketPropertiesProducer.getCompressMessageBodyThreshold());
        producer.setRetryAnotherBrokerWhenNotStoreOK(rocketPropertiesProducer.isRetryNextServer());

        return producer;
    }


    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(name = "rocketMQtemplet")
    public RocketMQTemplet rocketMQtemplet(DefaultMQProducer defaultMQProducer, RocketProperties rocketProperties) {
        RocketMQTemplet rocketMQTemplate = new RocketMQTemplet();
        rocketMQTemplate.setTopic(rocketProperties.getTopic());
        rocketMQTemplate.setProducer(defaultMQProducer);
        return rocketMQTemplate;
    }

}
