package com.the.rocket.mq;

import com.the.rocket.annotation.RocketMQListener;
import com.the.rocket.config.RocketProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhangzhi
 * Date: 2022/4/13 15:17
 */
@Slf4j
@Data
@Configuration
@EnableConfigurationProperties(RocketProperties.class)
public class RocketMQConsumerHandler implements ApplicationContextAware, SmartInitializingSingleton {

    @Autowired
    private StandardEnvironment environment;

    private RocketProperties rocketMQProperties;

    private ConfigurableApplicationContext applicationContext;

    private AtomicLong atomicLong = new AtomicLong(0);

    public RocketMQConsumerHandler(RocketProperties rocketMQProperties) {
        this.rocketMQProperties = rocketMQProperties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMQListener.class);
        if (Objects.nonNull(beans)) {
            beans.forEach(this::registerContainer);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    private void registerContainer(String beanName, Object bean) {
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        if (!MqListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " 不是实例 " + RocketMQListener.class.getName());
        }

        RocketMQListener annotation = clazz.getAnnotation(RocketMQListener.class);

        validate(annotation);

        String containerBeanName = String.format("%s_%s", RocketMQConsumerListenerContainer.class.getSimpleName(), atomicLong.incrementAndGet());

        GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;

        //注入容器
        genericApplicationContext.registerBean(containerBeanName, RocketMQConsumerListenerContainer.class,
                () -> createRocketMQListenerContainer(containerBeanName, bean, annotation));
        //获取容器
        RocketMQConsumerListenerContainer container = genericApplicationContext.getBean(containerBeanName,
                RocketMQConsumerListenerContainer.class);
        if (!container.isRunning()) {
            try {
                container.start();
            } catch (Exception e) {
                log.error("启动容器失败. {}", container, e);
                throw new RuntimeException(e);
            }
        }
        log.info("rocketmq 注册监听器注册bean, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
    }

    /**
     * 创建类
     *
     * @param name
     * @param bean
     * @param annotation
     * @return
     */
    private RocketMQConsumerListenerContainer createRocketMQListenerContainer(String name, Object bean, RocketMQListener annotation) {
        RocketMQConsumerListenerContainer container = new RocketMQConsumerListenerContainer(rocketMQProperties);
        container.setRocketMQListener(annotation);
        String nameServer = environment.resolvePlaceholders(annotation.nameserverAdder());
        nameServer = StringUtils.isEmpty(nameServer) ? rocketMQProperties.getNameSrvAddr() : nameServer;
        container.setNameSrvAddr(nameServer);

        container.setAccessChannel(annotation.accessChannel());

        container.setListener((MqListener) bean);

        /**
         * 容器名称
         */
        container.setName(name);

        return container;
    }

    private void validate(RocketMQListener annotation) {
        if (annotation.consumeMode() && annotation.messageModel() == MessageModel.BROADCASTING) {
            throw new BeanDefinitionValidationException("广播消息不支持有序获取消息!");
        }
    }
}
