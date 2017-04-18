/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.service;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.ymatou.mq.infrastructure.model.AppConfig;
import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.support.ConfigReloadListener;
import com.ymatou.mq.rabbit.RabbitConnectionFactory;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * 定时刷新配置 回调处理
 * Created by zhangzhihua on 2017/4/17.
 */
@Component
public class ConfigReloadService implements ConfigReloadListener {

    private static final Logger logger = LoggerFactory.getLogger(ConfigReloadService.class);

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private MessageConsumerManager messageConsumerManager;

    @PostConstruct
    public void init() {
        messageConfigService.addConfigCacheListener(this);
        //启动时consumer处理
        handleConsumer();
    }

    @Override
    public void callback() {
        //配置变化consumer处理
        handleConsumer();
    }

    /**
     * 处理consumer，若配置关闭则清除对应的consumer
     */
    void handleConsumer(){
        String[] clusters = {RabbitConstants.CLUSTER_MASTER,RabbitConstants.CLUSTER_SLAVE};
        Map<String,MessageConsumer> messageConsumerMap = MessageConsumerManager.getMessageConsumerMap();

        for(AppConfig appConfig:MessageConfigService.appConfigMap.values()){
            for(QueueConfig queueConfig:appConfig.getMessageCfgList()){
                for(CallbackConfig callbackConfig:queueConfig.getCallbackCfgList()){
                    //若配置关停，但目前存在运行着的messageConsumer则关停释放
                    if(!queueConfig.getEnable() || !callbackConfig.getEnable()){
                        for(String cluster:clusters){
                            String messageConsumerId = String.format("%s_%s",callbackConfig.getCallbackKey(),cluster);
                            if(messageConsumerMap.containsKey(messageConsumerId)){
                                logger.info("release message consumer {}",messageConsumerId);
                                messageConsumerManager.stopConsumer(callbackConfig.getCallbackKey(),cluster);
                            }
                        }
                    }

                    //若配置开启，但目前没有运行着的messageConsumer则启动
                    if(queueConfig.getEnable() && callbackConfig.getEnable()){
                        for(String cluster:clusters){
                            String messageConsumerId = String.format("%s_%s",callbackConfig.getCallbackKey(),cluster);
                            if(messageConsumerMap.get(messageConsumerId) == null){
                                logger.info("start message consumer callbackKey:{},cluster:{}",callbackConfig.getCallbackKey(),cluster);
                                messageConsumerManager.startConsumer(appConfig.getAppId(),queueConfig.getCode(),callbackConfig.getCallbackKey(),cluster);
                            }
                        }
                    }
                }
            }
        }
    }

}
